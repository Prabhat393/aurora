# 2014
# SAVI McGill: Heming Wen, Prabhat Tiwary, Kevin Han, Michael Smith,
#              Mike Kobierski and Hoai Phuoc Truong
#
"""The AP Monitor module tracks AP status and processes
received messages.

"""


import Queue
import datetime
import logging
import json
from pprint import pformat
import sys
import threading
import time
import traceback
from types import *
import uuid

from aurora import config_db
from aurora.exc import *
from aurora.cls_logger import get_cls_logger
from aurora.ap_provision import writer as provision
from aurora.stop_thread import *

KB = 1024**1
MB = 1024**2

LOGGER = logging.getLogger(__name__)


class APMonitor(object):
    """Handles AMQP response messages from access points."""
    SLEEP_TIME = 45
    # TODO: Make function to determine if dispatcher still exists
    def __init__(self, dispatcher, aurora_db, host, username, password):
        """Sets up and configures the environment required for message 
        passing using AMQP.  

        :param aurora.dispatcher.Dispatcher dispatcher:
        :param aurora.aurora_db.AuroraDB aurora_db:
        :param str host: RabbitMQ server IP 
        :param str username: RabbitMQ username 
        :param str password: RabbitMQ password 

        """
        self.LOGGER = get_cls_logger(self)
        # Configure dispatcher
        self.dispatcher = dispatcher
        self.dispatcher.set_timeout_callback(self.timeout)
        self.dispatcher.set_response_callback(self.process_response)
        self.dispatcher.set_close_pollers_callback(
            self.close_all_poller_threads)
        self.dispatcher.start_connection()

        self.aurora_db = aurora_db
        # self.ut = UptimeTracker(host, username, password)
        self.poller_threads = {}

        # To handle incoming status update requests, make a command queue
        self.timeout_queue = Queue.Queue()
        self._make_queue_daemon()

    def _make_queue_daemon(self):
        """Creates a queue daemon which watches for set_status calls
        and adds them to a FIFO queue.  This ensures the _set_status
        method does not get executed twice at the same time, which can
        cause the manager to place slices in a confused state.

        """
        self.LOGGER.info("Creating Queue Daemon...")
        self.qd = StoppableThread(target=self._watch_queue)
        self.qd.start()

    def _watch_queue(self, stop_event=None):
        """Target for the queue daemon.

        Executes until stop_event is set.

        :param threading.Event stop_event:

        """
        while True:
            # while len(self.timeout_queue) < 1 and not stop_event.is_set():
            #     time.sleep(0.1)
            if stop_event.is_set():
                self.LOGGER.info("Queue Daemon caught stop event")
                break

            try:
                (args, kwargs) = self.timeout_queue.get(block = False)
                self.LOGGER.debug("Queue Daemon is calling _set_status(%s, %s)",
                    args, kwargs
                )
                self._set_status(*args, **kwargs)
            except Queue.Empty as e:
                time.sleep(0.1)

    def _add_call_to_queue(self, *args, **kwargs):
        """Writes a set_status call to the queue.

        :param args: Arguments for set_status 
        :param kwargs: Keyword arguments for set_status 

        """
        self.timeout_queue.put((args, kwargs))

    def stop(self):
        """Stops all threads created by AP Monitor."""
        self.close_all_poller_threads()
        self.qd.stop()

    def close_all_poller_threads(self):
        """Stops all the AP poller threads created by AP Monitor."""
        self.LOGGER.debug("Closing all threads %s", self.poller_threads)
        for ap_name in self.poller_threads.keys():
            self._close_poller_thread(ap_name, 'admin')

    def _close_poller_thread(self, ap_name, unique_id):
        """Stops a single AP poller thread by ap_name."""
        if ap_name in self.poller_threads and unique_id == 'admin':
            poller_thread = self.poller_threads.pop(ap_name)
            self.LOGGER.info("Stopping thread %s %s", ap_name, poller_thread)
            poller_thread.stop()
            try:
                poller_thread.join()
            except RuntimeError as e:
                self.LOGGER.warn(e.message)


    def _build_slice_id_ssid_map(self, config):
        """Called from within process_response, this method will 
        find the ap_slice_id's on an access point from its returned 
        config, and will determine their associated SSID.

        :param dict config: Configuration returned from access point
        :rtype: dict

        """

        slice_id_ssid_map = {}
        for ap_slice_id, slice_cfg in \
                config["init_database"].iteritems():
            slice_ssid = None
            if ap_slice_id == "default_slice":
                continue
            for cfg in slice_cfg.get("RadioInterfaces", []):
                if cfg.get("flavor") == "wifi_bss":
                    slice_ssid = cfg["attributes"]["name"]
                    break
            slice_id_ssid_map[ap_slice_id] = slice_ssid
        return slice_id_ssid_map

    def process_response(self, channel, method, props, body):
        """Processes messages received from active access points.

        The responses are checked against previously sent message to 
        see if any correlation IDs match.  If yes, the response is 
        displayed along with the request originally sent, and is then 
        parsed to determine what the original message was.  The 
        databases are updated accordingly.

        :param pika.channel.Channel channel:
        :param pika.frame.Method.method method:
        :param pika.frame.Header.properties props:
        :param str body:

        """
        # Basic Proof-of-Concept Implementation
        # 1. We dispatch (see method above)
        # 2. Response received: if related to a request we sent out, OK
        # ACK it
        # Update database to reflect content (i.e. success or error)

        # If we don't have a record, that means that we already
        # handled a timeout previously and something strange happened to the AP
        # to cause it to wait so long. Reset it.

        # Check if we have a record of this ID
        have_request = False
        entry = None
        

        # Decode response
        decoded_response = json.loads(body)
        message = decoded_response['message']
        ap_name = decoded_response['ap']
        config = decoded_response['config']
        region = config['region']
        self.LOGGER.info("Receiving from %s...", ap_name)

        # Should wait for dispatcher to finish its dispatch method
        # before continuing.  It is possible to receive a response 
        # to a sent message before the message gets added to the
        # requests_sent list.  Waiting here avoids needless AP reset.
        if ap_name in self.dispatcher.lock:
            self.LOGGER.info("Locked for %s, waiting...", ap_name)
            while ap_name in self.dispatcher.lock:
                time.sleep(0.1)

        # First thing to do is cancel the timer, so an invalid timeout 
        # doesn't get triggered.
        self.LOGGER.debug("\nSent requests: %s",self.dispatcher.requests_sent)
        (have_request, entry) = self.dispatcher.have_request(
            props.correlation_id
        )
        request_correlation_id = None
        request_timer = None
        request_subject = None
        if have_request is not None and entry is not None:
            request_correlation_id = entry[0]
            request_timer = entry[1]
            request_subject = entry[2]
            self.LOGGER.debug("Cancelling timer %s", request_timer)
            request_timer.cancel()
            self.dispatcher.remove_request(request_correlation_id)
        else:
            self.LOGGER.warn("No request found for reply %s", props.correlation_id)
 
        self.LOGGER.debug("Pika channel: %s",channel)
        self.LOGGER.debug("Pika method: %s", method)
        self.LOGGER.debug(repr(props))
        self.LOGGER.debug(json.dumps(json.loads(body), indent=4))
        self.LOGGER.debug("\nSent requests: %s",self.dispatcher.requests_sent)

        if message == 'SYN':
            #TODO: If previous message has been dispatched and we are waiting 
            #      for a response, cancel the timer and/or send the command again
            # AP has started, check if we need to recreate slices
            self.LOGGER.info("%s has connected...", ap_name)
            self.aurora_db.ap_status_up(ap_name)
            self.dispatcher.remove_request(ap_syn=ap_name)

            slices_to_recreate = decoded_response['slices_to_recreate']
            self.recreate_slices(ap_name, slices_to_recreate)
            provision.update_last_known_config(ap_name, config)
            self.aurora_db.ap_syn_clean_deleting_status(ap_name)
            self.aurora_db.ap_update_hw_info(config['init_hardware_database'], 
                                             ap_name, region)
            self.start_poller(ap_name)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        elif message == 'SYN/ACK':
            self.LOGGER.info("%s responded to 'SYN' request", ap_name)
            if not have_request:
                self.LOGGER.warning("Warning: No request for received " +
                                    "'SYN/ACK' from %s", ap_name)
            
            provision.update_last_known_config(ap_name, config)
            self.aurora_db.ap_status_up(ap_name)
            self.aurora_db.ap_update_hw_info(config['init_hardware_database'], 
                                             ap_name, region)

            self.aurora_db.ap_syn_clean_deleting_status(ap_name)
            for ap_slice_id in (ap_slice_id for ap_slice_id in \
                    config['init_database'].keys() if \
                    ap_slice_id != 'default_slice'):
                try:
                    self.aurora_db.ap_slice_status_up(ap_slice_id)
                except AuroraException as e:
                    self.LOGGER.warn(e.message)
            self.start_poller(ap_name)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return


        elif message == 'FIN':
            self.LOGGER.info("%s is shutting down...", ap_name)
            try:
                self.set_status(None, None, None, False, ap_name)
                self.aurora_db.ap_update_hw_info(
                    config['init_hardware_database'], 
                    ap_name, region)
                # self.aurora_db.ap_status_down(ap_name)
                self.LOGGER.info("Updating config files...")
                provision.update_last_known_config(ap_name, config)
            except Exception as e:
                self.LOGGER.error(e.message)
            self.LOGGER.debug("Last known config:")
            self.LOGGER.debug(pformat(config))
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        if request_timer is not None:
            # A request for this message existed

            # decoded_response = json.loads(body)
            self.LOGGER.debug('Printing received message')
            self.LOGGER.debug(message)

            # For each slice in the returned message, determine its SSID
            # to update the SQL database.  Update SSID for each slice.
            slice_id_ssid_map = self._build_slice_id_ssid_map(config)
            for ap_slice_id, slice_ssid in slice_id_ssid_map.iteritems():
                self.aurora_db.ap_slice_set_ssid(ap_slice_id, slice_ssid)

            # Set status
            if request_subject is not None and request_subject != 'admin':
                ap_slice_id = request_subject
                successful = decoded_response['successful']
                self.set_status(None, ap_slice_id, 
                                successful, 
                                ap_name=ap_name)
                self.aurora_db.ap_update_hw_info(
                    config['init_hardware_database'], 
                    ap_name, region
                )
                
                self.LOGGER.info("Updating config files...")
                provision.update_last_known_config(ap_name, config)
                self.LOGGER.info("Updating config_db for slice %s", 
                                 ap_slice_id)
                slice_cfg = config['init_database'].get(ap_slice_id)
                slice_tenant = None
                for tenant_id, slice_list in \
                        config["init_user_id_database"].iteritems():
                    if ap_slice_id in slice_list:
                        slice_tenant = tenant_id
                        break

                try:
                    # TODO(Mike) Don't save if request unsuccessful
                    if successful:
                        config_db.save_config(slice_cfg, 
                                              ap_slice_id, 
                                              slice_tenant)
                except AuroraException as e:
                    LOGGER.error(e.message)
                
            else:
                if message == 'AP reset':
                    self.set_status('AP reset', None, None, True, ap_name)
                elif message == 'RESTARTING':
                    pass
                else:
                    self.set_status('slice_stats', 
                                    ap_slice_stats=message["ap_slice_stats"])
                    self.aurora_db.ap_update_hw_info(
                        config['init_hardware_database'], 
                        ap_name, region
                    )

        else:
            self.LOGGER.info("Sending reset to '%s'", ap_name)
            # Reset the access point
            self.reset_AP(ap_name)

        # Regardless of content of message, acknowledge receipt of it
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def timeout(self, ap_slice_id, ap_name, message_uuid=None):
        """This code will execute when a response is not received for 
        the command associated with the unique_id after a certain time 
        period.  It modifies the database to reflect the current status 
        of the AP.

        A timeout is serious: it is likely that the AP's OS has crashed, 
        or at least aurora is no longer running.

        :param str ap_slice_id:
        :param str ap_name:
        :param str message_uuid:

        """
        self.LOGGER.warn("A message timeout occured for %s (%s) %s",
                         ap_name, ap_slice_id, 
                         message_uuid if message_uuid is not None else '')
        if message_uuid is not None:
            self.dispatcher.remove_request(message_uuid)

        self.set_status(None, ap_slice_id, success=False, 
                        ap_up=False, ap_name=ap_name)

        # In the future we might do something more with the unique_id besides
        # identifying the AP, like log it to a list of commands that cause
        # AP failure, but for now it's good enough to know that our AP
        # has died and at least this command failed
        # If there are several commands waiting, this will execute several times
        # but all slices should already be marked
        # as deleted, down or failed, so there will not be any issue

    def update_records(self, message):
        """Update the traffic information of ap_slice.

        :param dict message: Message containing reported slice stats

        """
        self.LOGGER.debug("Updating records...")
        for ap_slice_id in message.keys():
            try:
                self.aurora_db.ap_slice_status_up(ap_slice_id)
            except Exception:
                traceback.print_exc(file=sys.stdout)

            try:
                self.aurora_db.ap_slice_update_time_stats(
                    ap_slice_id=ap_slice_id
                )
            except Exception:
                traceback.print_exc(file=sys.stdout)

            try:
                self.aurora_db.ap_slice_update_mb_sent(
                    ap_slice_id, 
                    float(message.get(ap_slice_id))/MB
                )
            except Exception:
                traceback.print_exc(file=sys.stdout)
            
    def set_status(self, cmd_category, *args, **kwargs):
        """Adds a call to the set_status queue, to be processes in order 
        of set_status calls.

        :param cmd_category: Determines which set_status method should 
            run, can be one of ``[None, "AP reset", "slice_stats"]``
        :param args: Arguments
        :param kwargs: Keyword Arguments

        """
        self.LOGGER.debug("Adding set_status call to queue for (%s, %s)", 
                          args, kwargs)
        self._add_call_to_queue(cmd_category, *args, **kwargs)

    def _set_status(self, cmd_category, *args, **kwargs):
        """Sets the status of the associated request in the
        database based on the previous status, i.e. pending -> active if
        create slice, deleting -> deleted if deleting a slice, etc.
        If the ap_up variable is false, the access point
        is considered to be offline and in an unknown state,
        so \*all\* slices are marked as such (down, failed, etc.).

        :param cmd_category: Determines which set_status method should 
            run, can be one of ``[None, "AP reset", "slice_stats"]``
        :param args: Arguments
        :param kwargs: Keyword Arguments

        """
        self.LOGGER.debug("Set status cmd category %s", cmd_category)
        if cmd_category is None:
            self._set_status_standard(*args, **kwargs)
        elif cmd_category == 'AP reset':
            self._set_status_reset(*args, **kwargs)
        elif cmd_category == 'slice_stats':
            self._set_status_stats(*args, **kwargs)

        return True

    def _set_status_stats(self, ap_slice_stats=None):
        """Sets the stats returned from an access point for its 
        associatd slices.

        :param dict ap_slice_stats:

        """
        try:
            self.update_records(ap_slice_stats)
        except:
            traceback.print_exc(file=sys.stdout)

    def _set_status_reset(self, unique_id, success,
                          ap_up=True, ap_name=None):
        """Executed upon receipt or timeout of an AP reset message.

        :param str unique_id:
        :param bool success:
        :param bool ap_up:
        :param str ap_name:

        """
        self.LOGGER.info("Processing AP reset")
        try:
            if ap_up:
                self.aurora_db.ap_status_up(ap_name)
            else:
                self.aurora_db.ap_status_down(ap_name)
            self.aurora_db.ap_down_slice_status_update(ap_name)
        except Exception as e:
            self.LOGGER.error(e.message)


    def _set_status_standard(self, unique_id, success, 
                             ap_up=True, ap_name=None):
        """The standard case for set status, this method takes care 
        of marking slices active, updating time stats associated with 
        them, and tracking AP status.

        :param str unique_id:
        :param bool success:
        :param bool ap_up:
        :param str ap_name:

        """
        if unique_id != 'SYN' and unique_id is not None:
            self.LOGGER.info("Updating ap status for ID %s.", str(unique_id))
        else:
            self.LOGGER.info("Updating ap status for ID %s.", str(ap_name))
        self.LOGGER.info("Request successful: %s", str(success))
        self.LOGGER.info("Access Point up: %s", str(ap_up))
        # Code:
        # Identify slice by unique_id
        # if ap_up:
        #   if pending and success, mark active
        #   else if deleting and success, mark deleted
        #   else if pending and failed, mark failed
        #   else if deleting and failed, mark failed (forcing user
        # to try deleting again or contact admin saying I can't delete;
        # this situation is so unlikely that if it happens an admin
        # really should come by and see what's going on)
        # else :
        # for all slices and/or commands relating to AP:
        #   if slice is active, mark down
        #   else if slice is deleting, mark deleted (will be when we reinitialize)
        #   else if slice is pending, mark failed
        try:
            if ap_up:
                # Access point is up - we are receiving individual packets
                self.aurora_db.ap_status_up(ap_name)
                self.aurora_db.ap_up_slice_status_update(unique_id, 
                                                         ap_name, success)
                self.aurora_db.ap_slice_update_time_stats(
                    ap_slice_id=unique_id)
            else:
                # Access point down, mark all slices and failed/down
                if ap_name is None:
                    ap_name = self.aurora_db.get_wslice_physical_ap(
                        ap_slice_id
                    )
                self.aurora_db.ap_slice_update_time_stats(ap_name=ap_name, 
                                                          ap_down=True)
                self.aurora_db.ap_status_down(ap_name)
                self.aurora_db.ap_down_slice_status_update(ap_name)
                self._close_poller_thread(ap_name, 'admin')

        except Exception, e:
            self.LOGGER.error(str(e))

    def recreate_slices(self, ap, slice_list):
        """Recreats the slices on their access point.

        This is different than restarting a slice, because the 
        configuration is assumed to already exist on the AP in question.

        :param str ap: Name of the access point
        :param list slice_list: List of slices to recreate

        """
        try:
            for ap_slice_id in slice_list:
                tenant_id = self.aurora_db.get_tenant_for_active_ap_slice(
                    ap_slice_id
                )
                self.LOGGER.debug("Returned tenant id %s", tenant_id)
                if tenant_id is not None:
                    self.LOGGER.info("%s for tenant %s", 
                                     ap_slice_id, tenant_id)
                    self.LOGGER.info("Recreating %s", ap_slice_id)
                    self.dispatcher.dispatch({'slice': ap_slice_id,
                                              'command': 'recreate_slice',
                                              'user': tenant_id
                                             },
                                             ap)
                else:
                    self.LOGGER.warn("No active slice %s", ap_slice_id)
        except Exception, e:
            traceback.print_exc(file=sys.stdout)

    def start_poller(self, ap_name):
        """Starts a poller to track the status of an access point.

        :param str ap_name: Name of the access point

        """
        poller_thread = self.poller_threads.get(ap_name)
        if poller_thread is None:
            poller_thread = TimerThread(target=self.poll_AP, args=(ap_name,))
            self.LOGGER.info("Starting poller for %s on thread %s", 
                             ap_name, poller_thread)
            self.poller_threads[ap_name] = poller_thread
            poller_thread.start()
        else:
            self.LOGGER.info("Poller thread for %s exists as %s", 
                             ap_name, poller_thread)

    def poll_AP(self, ap_name, stop_event=None):
        """Poller thread target: sends the access point a message in a 
        regular interval.  Stopped when stop_event is set, typically 
        when an access point goes offline.

        :param str ap_name: Name of the access point
        :param threading.Event stop_event:

        """
        #print "Timeout from Dispatcher", self.dispatcher.TIMEOUT
        STOP_EVENT_CHECK_INTERVAL = 0.2
        own_thread = self.poller_threads[ap_name]
        while ap_name in self.poller_threads:
            try:
                self.get_stats(ap_name)
            except AuroraException as e:
                self.LOGGER.warn(e.message)
            for i in range(int((self.dispatcher.TIMEOUT - 40)/STOP_EVENT_CHECK_INTERVAL)):
                if stop_event.is_set():
                    self.LOGGER.debug("Caught stop event for %s", own_thread)
                    break
                time.sleep(STOP_EVENT_CHECK_INTERVAL)
            if stop_event.is_set():
                self.LOGGER.debug("Poller thread for %s is dying now" % 
                                   ap_name)
                break
            self.LOGGER.debug("%s thread is %s", ap_name, own_thread)

    def reset_AP(self, ap):
        """Reset the access point.  If there are serious issues, however,
        a restart may be required.

        :param str ap:

        """
        self.aurora_db.ap_slice_update_time_stats(ap_name=ap, 
                                                  ap_down=True)
        self.aurora_db.ap_down_slice_status_update(ap) 
        try:
            self.dispatcher.dispatch(
                {
                    'slice':'admin', 
                    'command':'reset'
                }, 
                ap
            )
        except AuroraException as e:
            self.LOGGER.warn(e.message)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    def restart_AP(self, ap):
        """Restart the access point, telling the OS to reboot.

        :param str ap:

        """
        # The unique ID is fixed to be all F's for resets/restarts.
        self.aurora_db.ap_slice_update_time_stats(ap_name=ap, 
                                                  ap_down=True)
        self.aurora_db.ap_status_down(ap)
        self.aurora_db.ap_down_slice_status_update(ap) 
        try:
            self.dispatcher.dispatch(
                {
                    'slice':'admin', 
                    'command':'restart'
                },
                ap_name
            )
        except AuroraException as e:
            self.LOGGER.warn(e.message)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

    def get_stats(self, ap):
        """Query the access point for slice stats.

        :param str ap:

        """
        try:
            self.dispatcher.dispatch(
                {
                    'slice':'admin', 
                    'command':'get_stats'
                }, 
                ap
            )
        except AuroraException as e:
            self.LOGGER.warn(e.message)
        except Exception as e:
            traceback.print_exc(file=sys.stdout)

#for test
#if __name__ == '__main__':
#    host = 'localhost'
#    mysql_username = 'root'
#    mysql_password = 'supersecret'
#    manager = APMonitor(None, host , mysql_username, mysql_password)
#    manager.set_status(12, True)
