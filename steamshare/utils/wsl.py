from steamshare.utils.shared import (
    ClassicShared,
    StaticShared
)
from steamshare.webstomp.utils import StompUtils
from steamshare.webstomp import exceptions
import traceback
import random
import trio
import os


class StompMessage(object):
    def __init__(self, id, receipt_id, message, origin):
        self.id = id
        self.receipt_id = receipt_id
        self.message = message
        self.origin = origin


class WebSocketLib(object):
    @staticmethod
    async def unsubscriber(logger,
                           websocket,
                           id,
                           timeout,
                           retries,
                           stomp_protocol_manager):

        unsubscribe_successful = False
        n = 0
        while n < retries:
            try:
                logger.debug('Unsubscribe attempt #{}'.format(n + 1))
                unsubscribe_cmd = stomp_protocol_manager.unsubscribe(id)
                await WebSocketLib.sender(logger, websocket, unsubscribe_cmd,
                                          timeout)
            except Exception as e:
                logger.debug('Unsubscribe operation on {} failed due to the '
                             'following exception\n{}'.format(queue, e))
            else:
                unsubscribe_successful = True
                break
            n += 1

        if not unsubscribe_successful:
            logger.debug('Unsubscribe on queue {} failed after {} attempts'
                         ''.format(queue, retries))

    @staticmethod
    async def subscriber(
        logger,
        websocket,
        id,
        queue,
        timeout,
        retries,
        purge_before_subscribe,
        stomp_protocol_manager,
        http_client
    ):

        if purge_before_subscribe:
            http_client.purge_queue(queue)

        subscribe_successful = False
        n = 0
        while n < retries:
            try:
                logger.debug('Subscribe attempt #{}'.format(n + 1))
                subscribe_cmd = stomp_protocol_manager.subscribe(queue, id)
                await WebSocketLib.sender(logger, websocket, subscribe_cmd,
                                          timeout)
            except Exception as e:
                logger.debug('Subscribe operation on {} failed due to the '
                             'following exception\n{}'.format(queue, e))
            else:
                subscribe_successful = True
                break
            n += 1

        if not subscribe_successful:
            logger.debug('Subscribe on queue {} failed after {} attempts'
                         ''.format(queue, retries))

    @staticmethod
    async def receiver(logger, websocket, timeout):
        with trio.fail_after(timeout):
            message = await websocket.get_message()

        frame = StompUtils.parse_frame(StompUtils.encode(message))
        if not frame:
            raise exceptions.EmptyFrameException()

        logger.debug('Received frame type: %r, headers=%r, body=%r',
                     frame.cmd, frame.headers, frame.body)

        if frame.cmd.lower() == 'error':
            raise exceptions.ErrorFrameReceivedException('headers=%r, '
                                                         'body=%r',
                                                         frame.headers,
                                                         frame.body)

        return frame

    @staticmethod
    async def sender(logger, websocket, message, timeout):
        with trio.fail_after(timeout):
            logger.debug('Sending {}'.format(message))
            await websocket.send_message(message)

    @staticmethod
    async def publisher(logger, websocket, timeout, interval,
                        stomp_protocol_manager, msg_cnt):
        logger.debug('Indefinite publisher started...')
        n = 0
        while True:
            try:
                logger.debug('Attempting round #{} of burst publishing'
                             ''.format(n + 1))
                for _ in range(msg_cnt):
                    content = 'message {}'.format(random.randint(100,
                                                                 1000000))
                    message = stomp_protocol_manager.send(content)
                    await trio.sleep(0.001)
                    await WebSocketLib.sender(logger, websocket, message,
                                              timeout)
            except trio.TooSlowError:
                logger.error('Message publisher timed out\nIndefinite '
                             'publishing attempt halted')
                break
            except Exception as e:
                logger.debug('Message publisher failed due to the following '
                             'error:\n{}\nIndefinite publishing attempt '
                             'halted!!!'.format(e))
                break
            n += 1
            logger.debug('Sleeping for {} seconds before round #{} of burst '
                         'publishing'.format(interval, n + 1))
            await trio.sleep(interval)

    @staticmethod
    async def connect(logger, websocket, cmd, timeout, interval,
                      retry_attempts):
        right_frame_type = False
        n = 0
        while not right_frame_type and n < retry_attempts:
            try:
                logger.debug('Minor connection attempt #{}'.format(n + 1))
                await WebSocketLib.sender(logger, websocket, cmd, timeout)
                frame = await WebSocketLib.receiver(logger, websocket,
                                                    timeout)
            except trio.TooSlowError:
                fail_reason = 'conection attempt timed out after {}'\
                    ' seconds'.format(interval)
            except exceptions.EmptyFrameException:
                fail_reason = 'empty frame received'
            except exceptions.ErrorFrameReceivedException as e:
                fail_reason = 'following error frame received\n{}'.format(e)
            except Exception as e:
                fail_reason = 'following unexpected exception/error occured '\
                    'while attempting to receive a message\n{}'.format(e)
            else:
                frame_cmd = frame.cmd.lower()
                right_frame_type = frame_cmd == 'connected' if frame \
                    else right_frame_type
                if right_frame_type:
                    break
                else:
                    fail_reason = 'expecting a CONNECTED frame type, but '\
                        'instead received the following frame type'\
                        ': {}'.format(frame_cmd)

            logger.debug('Protocol connection attempt failed due to the '
                         'following reason: {}'.format(fail_reason))

            logger.debug('Sleeping for {} seconds before the next minor '
                         'protocol connection attempt'.format(interval))

            await trio.sleep(interval)

            n += 1

        if not right_frame_type:
            raise exceptions.ConnectFailedException()

        logger.debug('Protocol connection SUCCESSFUL !!')

    @staticmethod
    async def protocol_heartbeat(
        logger,
        websocket,
        timeout,
        interval,
        enable_protocol_heartbeat_logging
    ):
        while True:
            with trio.fail_after(timeout):
                if enable_protocol_heartbeat_logging:
                    logger.debug('Sending empty heartbeat frame')
                await websocket.send_message('')
            await trio.sleep(interval)

    @staticmethod
    async def websocket_heartbeat(logger, websocket, timeout, interval,
                                  enable_ping_logging):
        while True:
            with trio.fail_after(timeout):
                if enable_ping_logging:
                    logger.debug('Pinging')
                await websocket.ping()
            await trio.sleep(interval)

    @staticmethod
    async def connector(
        logger,
        websocket,
        timeout,
        minor_interval,
        major_interval,
        retry_attempts,
        stomp_protocol_manager
    ):
        not_connected = True
        n = 0
        connect_cmd = stomp_protocol_manager.connect()
        while not_connected:
            logger.debug('Protocol major connection attempt {}'.format(n + 1))
            try:
                await WebSocketLib.connect(logger,
                                           websocket,
                                           connect_cmd,
                                           timeout,
                                           minor_interval,
                                           retry_attempts)
            except Exception as e:
                logger.debug('Waiting for {} seconds before the next major '
                             'protocol connection attempt due to the '
                             'following exception:\n{}'.format(
                                 major_interval, e))
            else:
                break
            await trio.sleep(major_interval)
            n += 1

    @staticmethod
    async def listener(
        logger,
        websocket,
        nursery,
        processor,
        arbitrary_wait_period,
        listener_timeout,
        disconnect_timeout,
        log_indefinite_listening_attempt,
        stomp_protocol_manager
    ):
        logger.debug('Indefinite Listener started...')
        n = 0
        while True:
            if log_indefinite_listening_attempt:
                logger.debug('Indefinite Listening attempt #{}'.format(n + 1))
            try:
                frame = await WebSocketLib.receiver(logger, websocket,
                                                    listener_timeout)
                (frame_type, frame_headers, frame_body) = (frame.cmd.lower(),
                                                           frame.headers,
                                                           frame.body)
            except trio.TooSlowError:
                logger.error('Message receiver timed out\nIndefinite Listening'
                             ' attempt halted')
                break
            except exceptions.EmptyFrameException as e:
                pass
            except exceptions.ErrorFrameReceivedException as e:
                logger.error('Received following error frame \n{}\nIndefinite '
                             'Listening attempt halted!!!\n'.format(
                                 traceback.format_exc()))
                receipt_id = StompUtils.get_uuid()
                disconnect_msg = stomp_protocol_manager.disconnect(receipt_id)
                await WebSocketLib.sender(logger, websocket, disconnect_msg,
                                          disconnect_timeout)
                await trio.sleep(arbitrary_wait_period)
                raise
            except Exception as e:
                logger.error('Receiver failed due to the following error\n{}\n'
                             'Indefinite Listening attempt halted'.format(
                                 traceback.format_exc()))
                break
            else:
                if frame_type == 'message':
                    receipt_id = StompUtils.get_uuid()
                    message_id = frame_headers['message-id']
                    message_queue = frame_headers['destination'].split(
                        '/')[-1]
                    message = frame.body
                    logger.debug('RAW MESSAGE RECEIVED =====> {}'.format(
                        message))
                    formatted_message = StompUtils.decode(message.rstrip(

                    ).rstrip(
                        StompUtils.NULL))
                    stomp_message = StompMessage(message_id,
                                                 receipt_id,
                                                 formatted_message,
                                                 message_queue)
                    nursery.start_soon(
                        processor,
                        websocket,
                        nursery,
                        stomp_message
                    )
                elif frame_type == 'receipt':
                    receipt_id = frame_headers['receipt-id']
                    stomp_protocol_manager.mark_receipt_read(receipt_id)
            n += 1

    @staticmethod
    async def acknowledge(logger,
                          websocket,
                          message_id,
                          receipt_id,
                          stomp_protocol_manager,
                          send_timeout,
                          receipt_timeout,
                          send_retries,
                          receipt_retries
                          ):
        ack_msg = stomp_protocol_manager.ack(message_id, receipt_id)
        acknowledgement_received_by_server = False

        n = 0
        while not acknowledgement_received_by_server and \
                n < receipt_retries:
            stomp_protocol_manager.delete_receipt(receipt_id)
            stomp_protocol_manager.set_receipt(receipt_id)

            logger.debug(
                'Server acknowledgement major attempt #{}'.format(n + 1))
            acknowledgement_received_by_server = await \
                WebSocketLib.acknowledgement_sender(
                    logger,
                    websocket,
                    ack_msg,
                    message_id,
                    receipt_id,
                    stomp_protocol_manager,
                    send_timeout,
                    receipt_timeout,
                    send_retries)
            n += 1

        stomp_protocol_manager.delete_receipt(receipt_id)
        logger.debug('acknowledgement_received_by_server: {}'.format(
            acknowledgement_received_by_server))

        if not acknowledgement_received_by_server:
            logger.debug('Message id: {}, Receipt id: {} processed but '
                         'acknowledgement failed after {} major attempts'
                         '!'.format(message_id, receipt_id, n))

            return

        logger.debug('Message id: {}, Receipt id: {} processed and '
                     'acknowledged successfully after {} major attempts'
                     '!'.format(message_id, receipt_id, n))

    @staticmethod
    async def acknowledgement_sender(logger,
                                     websocket,
                                     ack_msg,
                                     message_id,
                                     receipt_id,
                                     stomp_protocol_manager,
                                     send_timeout,
                                     receipt_timeout,
                                     send_retries):
        acknowledgement_sent = False
        acknowledgement_received_by_server = False

        n = 0
        while not acknowledgement_sent and n < send_retries:
            try:
                logger.debug('Acknowledgement message send attempt #{} to '
                             'server for message id: {}'.format(n + 1,
                                                                message_id))
                await WebSocketLib.sender(logger, websocket, ack_msg,
                                          send_timeout)
            except trio.TooSlowError:
                logger.debug(
                    'Acknowledgement message send failed due to timeout')
            else:
                acknowledgement_sent = True
            n += 1

        if acknowledgement_sent:
            logger.debug('Acknowledgement message sent to server successfully'
                         ' for message id {} after {} attempts'.format(
                             message_id, n))

            try:
                with trio.fail_after(receipt_timeout):
                    receipt = stomp_protocol_manager.get_receipt(receipt_id)
                    await receipt.wait()
            except trio.TooSlowError:
                logger.debug('Acknowledgement receipt for message id {} and '
                             'receipt id {} combo timed out'.format(
                                 message_id,
                                 receipt_id))
            else:
                acknowledgement_received_by_server = True
                logger.debug('Acknowledgement receipt for message id {} and '
                             'receipt id {} GRAND SUCCESS!!'.format(
                                 message_id,
                                 receipt_id))

        return acknowledgement_received_by_server

    @staticmethod
    async def mass_subscriber(logger,
                              websocket,
                              nursery,
                              subscription_queues,
                              subscriber_id_generator,
                              subscriber_timeout,
                              subscriber_retries,
                              purge_before_subscribe,
                              stomp_protocol_manager,
                              rmq_http_client):
        for queue in subscription_queues:
            logger.debug('Mass Subscriber: Subscribing to queue =--> {}'
                         ''.format(queue))
            nursery.start_soon(
                WebSocketLib.subscriber,
                logger,
                websocket,
                next(subscriber_id_generator),
                queue,
                subscriber_timeout,
                subscriber_retries,
                purge_before_subscribe,
                stomp_protocol_manager,
                rmq_http_client
            )

    async def websocket_runner(logger,
                               url,
                               ssl_context,
                               arbitrary_wait_period,
                               websocket_ping_timeout,
                               websocket_ping_interval,
                               enable_websocket_heartbeat_logging,
                               webstomp_runner
                               ):
        async with open_websocket_url(url,
                                      ssl_context=ssl_context
                                      ) as websocket:
            await trio.sleep(arbitrary_wait_period)
            async with trio.open_nursery() as nursery:
                nursery.start_soon(
                    WebSocketLib.websocket_heartbeat,
                    logger,
                    websocket,
                    websocket_ping_timeout,
                    websocket_ping_interval,
                    enable_websocket_heartbeat_logging
                )
                nursery.start_soon(webstomp_runner, websocket, nursery)

    async def safe_websocket_runner(logger,
                                    url,
                                    log_dir,
                                    ssl_context,
                                    stats_file_prefix,
                                    arbitrary_wait_period,
                                    websocket_ping_timeout,
                                    websocket_ping_interval,
                                    stomp_protocol_interval_major,
                                    enable_websocket_heartbeat_logging,
                                    webstomp_runner,
                                    loggers
                                    ):
        classic_shared_lib = ClassicShared(logger)
        n = 0
        while True:
            try:
                logger.debug('Websocket connection attempt #{} to {}'
                             ''.format(n + 1, self.config.uri))
                stats_filename = '{}_{}.log'.format(
                    stats_file_prefix,
                    classic_shared_lib.zero_padded(n + 1))
                stats_logfile = os.path.join(log_dir, stats_filename)
                loggers['stats'] = StaticShared.get_logger(
                    'stats',
                    log_to_file=True,
                    log_file_name=stats_logfile,
                    log_file_mode='a'
                )
                await WebSocketLib.websocket_runner(
                    logger,
                    url,
                    ssl_context,
                    arbitrary_wait_period,
                    websocket_ping_timeout,
                    websocket_ping_interval,
                    enable_websocket_heartbeat_logging,
                    webstomp_runner
                )
            except (
                ConnectionClosed,
                ConnectionRejected,
                HandshakeError,
                trio.MultiError,
                trio.ClosedResourceError,
                exceptions.ErrorFrameReceivedException,
                ValueError
            ):
                logger.debug('Websocket connection attempt to {} BROKEN'
                             ' or FAILED !!'.format(url))
                logger.debug('Websocket connection error details\n{}'
                             ''.format(traceback.format_exc()))
                logger.debug('Retrying websocket connection to {} in {} '
                             'seconds'.format(url,
                                              stomp_protocol_interval_major))
                await trio.sleep(stomp_protocol_interval_major)
            else:
                logger.debug('Websocket connection attempt #{} to {} '
                             'SUCCESSFUL !!'.format(n + 1, url))
                break
            n += 1
