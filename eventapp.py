
from decorator import decorator
from inspect import getargspec, isbuiltin, getmembers, ismethod
from itertools import chain
from time import sleep
from redis_natives import datatypes as rn
from contextlib import contextmanager
import sys

import bubbles

from threading import Thread
from multiprocessing import Process, Event, Lock


# first handler's args based on incoming events kwarg based args
# any of the initial event's data not handled by initial handler
#   are excluded from further contexts
# lastt handlers yield values should be dicts to map to outgoing event
# all internal state's are done by position sig kwargs
# if handler takes less args than current stack handler is assumed
#   to want newest args (end of stack)
# handlers which return Bools are treated as filterse

import revent.introspect_data as rc_introspect

threads_per_stage = 5
forks = 5

class EventApp(object):
    def __init__(self, app_name, config,
                 base_context_mapping, *stage_definitions):

        self.app_name = app_name
        self.config = config
        self.stage_definitions = stage_definitions
        self.base_context_mapping = base_context_mapping

        self.stages = []
        self.create_stages()

    def run(self, threaded=True, multiprocess=False):
        """
        starts up a child thread for each stage
        """

        if multiprocess:
            self._run_multiprocess(threaded)
        elif threaded:
            self._run_threaded()
        else:
            self._run()

    def _run(self):
        """
        runs all handlers in single thread
        """

        try:
            while True:
                for stage in self.stages:
                    stage.cycle(block=True, timeout=1)
        except KeyboardInterrupt, ex:
            print 'Caught keyboard'
            pass
        except Exception, ex:
            print 'Exception: %s' % ex
            raise

        print 'stopping'

    def _run_multiprocess(self, threaded=False, stop_event=None):
        """
        run the handlers in their own processes
        """

        global forks

        def process_run(app_handler, stop_event):
            while not stop_event.is_set():
                try:
                    app_handler.cycle(block=True, timeout=10)
                except Exception, ex:
                    print 'EXCEPTION: %s' % str(ex)
                    stop_event.set()
                    raise
            print 'DONE process'

        # set up a event so we can stop all the handlers gracefully
        if not stop_event:
            stop_event = Event()

        # create a process for each handler
        processes = []
        print 'creating processes'
        for j in xrange(forks):
            for i, stage in enumerate(self.stages):
                if not threaded:
                    process = Process(target=process_run, args=(stage, stop_event))
                else:
                    process = Process(target=self._run_threaded,
                                      args=(stop_event,))
                processes.append(process)


        # start our processes
        print 'starting processes'
        for process in processes:
            process.start()

        # now chill about waiting for an interupt
        try:
            while True and not stop_event.is_set():
                sleep(1)
        except Exception, ex:
            print 'EXCEPTION: %s' % (ex)
        except KeyboardInterrupt, ex:
            print 'caught keyboard interrupt'

        print 'stopping processes'

        # stop all the processes
        stop_event.set()
        for process in processes:
            process.join()

        raise ex


    def _run_threaded(self, stop_event=None):
        """
        runs handlers in their own threads
        """

        global threads_per_stage

        def thread_run(app_handler, stop_event):
            while not stop_event.is_set():
                try:
                    app_handler.cycle(block=True, timeout=10)
                except Exception, ex:
                    print 'EXCEPTION: %s' % str(ex)
                    stop_event.set()
                    raise
            print 'DONE THREAD'

        # set up a event so we can stop all the handlers gracefully
        if not stop_event:
            stop_event = Event()

        # create a thread for each handler
        threads = []
        print 'creating threads'
        for i, stage in enumerate(self.stages):
            for j in xrange(threads_per_stage):
                thread = Thread(target=thread_run, args=(stage, stop_event))
                threads.append(thread)

        # start our threads
        print 'starting threads'
        for thread in threads:
            thread.start()

        # now chill about waiting for an interupt
        try:
            while True and not stop_event.is_set():
                sleep(1)
        except Exception, ex:
            print 'EXCEPTION: %s' % (ex)
        except KeyboardInterrupt, ex:
            print 'caught keyboard interrupt'

        print 'stopping threads'

        # stop all the threads
        stop_event.set()
        for thread in threads:
            thread.join()

        raise ex

    def create_stages(self):

        # we are going to create the stages in multiple passes
        # we do passes until every stage has an in and out event
        print 'creating stages: %s' % str(self.stage_definitions)

        # go through the stage defs, creating a stage for each
        # create inline list of stages missing their in event or out event
        c = 0
        def incomplete():
            incomplete_stages = [s for s in self.stages if not s.in_event or not s.out_event]
            #if incomplete_stages:
                #print 'INCOMPLETE STAGES: %s' % (str(incomplete_stages))
            #if len(self.stages) != len(self.stage_definitions):
                #print 'INCOMPLETE STAGES LEN'
            return incomplete_stages or len(self.stages) != len(self.stage_definitions)

        while incomplete():
            print 'PASS %s' % c; c+=1
            for i, stage_def in enumerate(self.stage_definitions):
                print 'STAGE: %s :: %s' % (i, str(stage_def))

                try:
                    next_stage = self.stages[i+1]
                except IndexError:
                    next_stage = None

                # limit begining of seek range to begining of list
                # since python list index's can be negative
                if i-1 < 0:
                    previous_stage = None
                else:
                    previous_stage = self.stages[i-1]

                stage = self._create_stage(stage_def,
                                           previous_stage, next_stage)

                print '>> RESULT: %s' % stage

                try:
                    self.stages[i] = stage
                except IndexError:
                    self.stages.append(stage)

        return self.stages


    def _create_stage(self, stage_def, previous_stage=None, next_stage=None):

        handler = in_event = out_event = None

        print '---creating stage: \n---%s\n---%s\n---%s---' % (previous_stage, stage_def, next_stage)

        # TODO: update to support multiple handlers per stage def which
        #       would result in multiple stages being created
        # TODO: update to support multiple in events

        # if the stage def isn't complete (doesn't have three args)
        # than we are going to add in the out / in events from the handlers
        # on either side
        full_def = stage_def
        if not len(stage_def) >= 3:
            full_def = chain([previous_stage.out_event] if previous_stage else [],
                             stage_def,
                             [next_stage.in_event] if next_stage else [])


        for arg in full_def:

            print 'ARG: %s' % arg

            if not handler and callable(arg):
                handler = arg

            elif not in_event and not callable(arg):
                in_event = arg

            elif not out_event and not callable(arg):
                out_event = arg

        # fill in missing pieces
        if not in_event and previous_stage:
            in_event = previous_stage.out_event

        if not out_event and next_stage:
            out_event = next_stage.in_event

        # make sure we've got everything
        assert handler, "No handler found for stage: " + str(stage_def)
        assert in_event, "No in event found: " + str(stage_def)

        # finally, create our handler
        return AppHandler(self.app_name, self.config,
                          self.base_context_mapping, in_event,
                          handler, out_event)


class AppHandler(object):

    def __init__(self, app_name, config, base_context_mapping,
                 in_event, handler, out_event=None):

        # import here so that we import post-fork
        from revent import ReventClient, ReventMessage
        from redis import Redis

        self.config = config
        self.app_name = app_name
        self.in_event = in_event
        self.handler = handler
        self.out_event = out_event
        self.context = bubbles.Context(base_context_mapping)
        self.ReventMessage = ReventMessage

        print 'context mapping: %s' % self.context.mapping

        # sanity checks
        assert in_event, "Must provide in_event"
        assert handler, "Must provide handler"

        # subscribe to our in_event
        self.channel = '%s-%s-%s' % (app_name, in_event, self.handler.__name__)
        self.rc = ReventClient(self.channel, in_event, verified=10,
                               **self.config.get('revent', {}))

        # create a connection to redis
        self.redis = Redis(**self.config.get('redis', {}))

        # make our redis namespace the same as our channel
        self.redis_ns = 'App-%s' % self.app_name

        # update the context to include all the underscore methods
        # (but not dunderscore)
        for name, value in getmembers(self):
            if self._include_in_context(name, value):
                self.context.add(name, value)

        # update the context to include the revent client and
        # introspect module
        self.context.add('revent_client', self.rc)
        self.context.add('introspect', rc_introspect)

    @staticmethod
    def _include_in_context(name, value):
        include = name.startswith('_') and not name.startswith('__')
        return include

    # helper methods for accessing natives
    def _dict(self, name, default=None):
        name = str(name) # redis demands ascii
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.Dict(*args)

    def _sequence(self, name, default=None):
        name = str(name)
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.Sequence(*args)

    def _zset(self, name, default=None):
        name = str(name)
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.ZSet(*args)

    def _list(self, name, default=None):
        name = str(name)
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.List(*args)

    def _set(self, name, default=None):
        name = str(name)
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.Set(*args)

    def _string(self, name, default=None):
        name = str(name)
        args = [self.redis, '%s:%s' % (self.redis_ns, name)]
        if default is not None:
            args.append(default)
        return rn.Primitive(*args)

    def _signal(self, name):
        name = str(name)
        return Signal(self.redis,
                      '%s:%s:signal' % (self.redis_ns, name))

    def _stop(self):
        print 'Stopping handler'
        raise StopIteration

    def __repr__(self):
        return '<AppHandler %s=>%s=>%s>' % (self.in_event,
                                            self.handler.__name__,
                                            self.out_event or '')

    def cycle(self, block=False, timeout=1):

        # grab up our event
        event = self.rc.read(block=block, timeout=timeout)

        if event:

            # wrap the handler in the current context
            wrapped_handler = self._wrap_handler(event)

            # call our handler
            try:
                print '[H] %s [E] %s' % (self.handler.__name__, event)
                for result in wrapped_handler():

                    # see if this results calls for another event to be fired
                    result_event = self._build_result_event(event, result)

                    print '[%s] [%s] %s => %s' % (event, self, str(result), str(result_event))

                    # result event is event, event_data
                    if result_event:
                        self.rc.fire(*result_event)

            except Exception:
                print 'Handler Exception: %s %s' % (self.handler.__name__, event)
                raise

            # flush the prints
            sys.stdout.flush()

            # let them know we're done handling the event
            self.rc.confirm(event)

    def _wrap_handler(self, event):
        """
        returns the handler wrapped in a context which
        includes the current event's data
        """
        context = self.context.copy()
        context.update(
            event_data=event.data,
            event_name=event.event,
            event_id=event.id,
            event=event
        )
        context.update(**event.data)
        return context.create_partial(self.handler)

    def _build_result_event(self, event, result):

        # if they didn't define an out event than we aren't putting
        # off events even if we get a result
        if not self.out_event:
            return None

        # if the result if an event, just fire it's info
        if isinstance(result, self.ReventMessage):
            return result.event, result.data

        # if the reuslt is a true or false than it's a filter
        # a false means don't re-fire the event, True means re-fire
        # if we have an out event set than we'll fire the input event's
        # data w/ our out event name
        if result is True:
            return self.out_event, event.data

        # if the result if false than we're filting the message
        if result is False:
            return None

        # if the reuslt is a dictionary than we're going to use that
        # dict as the resulting event's data
        if isinstance(result, dict):
            return self.out_event, result

        # if it's a two item tuple it's either new event
        # or update to source event's data and out event
        if isinstance(result, tuple) and len(result) == 2 \
           and isinstance(result[0], (str, unicode)):

            # if the second value is a dict, than it's a new event
            # and the first value is the new event's event
            if isinstance(result[1], dict):
                return tuple(result)

            else:
                # a k/v pair to set in the previous
                # events data (k,v)

                # update the data in place, no one else should touch!
                event.data[result[0]] = result[1]
                return self.out_event, event.data

        elif isinstance(result, tuple):

            # if it's a tuple which is longer than 2 than it should be
            # full of sub tuples, each sub tuble contains a key/value
            # to be set
            for k, v in result:
                event.data[k] = v
            return self.out_event, event.data

        # if it's anything else we're going to update the source event's
        # data to include these results and use resuling data as new
        # events data
        event_data = event.data.copy()
        previous_results = event_data.setdefault('results', [])
        if isinstance(result, (list, tuple)):
            previous_results.extend(result)
        else:
            previous_results.append(result)

        return self.out_event, event_data




class Signal(object):
    """
    simple way of keeping magnitude
    """

    def __init__(self, rc, name):
        self.name = name
        self.key = 'signal:%s' % self.name
        self.rc = rc

    def _get_value(self):
        return int(self.rc.get(self.key) or 0)

    def _set_value(self, value):
        self.rc.set(self.key, value)

    value = property(_get_value, _set_value)

    def reset(self):
        return self.rc.set(self.key, 0)

    def incr(self, change=1):
        return self.rc.incr(self.key, change)

    def decr(self, change=1):
        return self.rc.decr(self.key, change)
