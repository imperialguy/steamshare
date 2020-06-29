import trio

class WebSocketLib(object):
    @staticmethod
    async def subscriber(logger, websocket, queue, timeout, retries,
        webstomp_client, http_client, purge_before_subscribe=False):

        if purge_before_subscribe:
            http_client.purge_queue(queue)

        subscribe_successful = False
        n = 0
        while n < retries:
            try:
                logger.debug('Subscribe attempt #{}'.format(n + 1))
                subscribe_cmd = webstomp_client.subscribe(queue)
                await sender(websocket, subscribe_cmd, timeout)
            except Exception as e:
                logger.debug('Subscribe operation on {} failed due to the '
                    'following exception\n{}'.format(
                    queue, e))
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

        frame = utils.parse_frame(utils.encode(message))
        if not frame:
            raise exceptions.EmptyFrameException()

        logger.debug('Received frame type: %r, headers=%r, body=%r',
                    frame.cmd, frame.headers, frame.body)

        if frame.cmd.lower() == 'error':
            raise exceptions.ErrorFrameReceivedException('headers=%r, '
                'body=%r', frame.headers, frame.body)

        return frame

    @staticmethod
    async def sender(logger, websocket, message, timeout):
        with trio.fail_after(timeout):
            logger.debug('Sending {}'.format(message))
            await websocket.send_message(message)

    @staticmethod
    async def publisher(logger, websocket, timeout, interval, stomp_protocol, msg_cnt):
        logger.debug('Indefinite publisher started...')
        n = 0
        while True:
            try:
                logger.debug('Attempting round #{} of burst publishing'.format(
                    n + 1))
                for _ in range(msg_cnt):
                    content = 'message {}'.format(random.randint(100, 1000000))
                    message = stomp_protocol.send(content)
                    await trio.sleep(0.001)
                    await sender(websocket, message, timeout)
            except trio.TooSlowError as e:
                logger.error('Message publisher timed out\nIndefinite publishing'
                    ' attempt halted')
                break
            except Exception as e:
                logger.debug('Message publisher failed due to the following '
                    'error\nIndefinite publishing attempt halted')
                break
            n += 1
            logger.debug('Sleeping for {} seconds before round #{} of burst '
                'publishing'.format(interval, n + 1))
            await trio.sleep(interval)

    @staticmethod
    async def connect(logger, websocket, cmd, timeout, interval, retry_attempts):
        right_frame_type = False
        n = 0
        while not right_frame_type and n < retry_attempts:
            try:
                logger.debug('Minor connection attempt #{}'.format(n + 1))
                await sender(websocket, cmd, timeout)
                frame = await receiver(websocket, timeout)
            except trio.TooSlowError as e:
                fail_reason = 'conection attempt timed out after {}'\
                                ' seconds'.format(interval)
            except exceptions.EmptyFrameException as e:
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

            logger.debug('Protocol connection attempt failed due to the following'
                ' reason: {}'.format(fail_reason))

            logger.debug('Sleeping for {} seconds before the next minor protocol'
                ' connection attempt'.format(interval))

            await trio.sleep(interval)

            n += 1

        if not right_frame_type:
            raise exceptions.ConnectFailedException()

        logger.debug('Protocol connection SUCCESSFUL !!')

    @staticmethod
    async def protocol_heartbeat(logger, websocket, timeout, interval,
            enable_protocol_heartbeat_logging):
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
    async def connector(logger, websocket, timeout, minor_interval, major_interval,
            retry_attempts, stomp_protocol):
        not_connected = True
        n = 0
        connect_cmd = stomp_protocol.connect()
        while not_connected:
            logger.debug('Protocol major connection attempt {}'.format(n + 1))
            try:
                await connect(websocket, connect_cmd, timeout,
                      minor_interval, retry_attempts)
            except Exception as e:
                logger.debug('Waiting for {} seconds before the next major '
                    'protocol connection attempt'.format(major_interval))
            else:
                break
            await trio.sleep(major_interval)
            n += 1

    @staticmethod
    async def listener(logger, processor, websocket, timeout, minor_interval,
            major_interval, stomp_protocol, nursery, lock,
            acknowledgement_receipt_sleep, dynamic_queue,
            acknowledgement_receipt_retries, acknowledgement_send_retries,
            deployment_factory, manager, log_indefinite_listening_attempt,
            enable_process_life_logging, deployment_poll_interval,
            deployment_wait_interval, purge_before_subscribe, http_client):
        logger.debug('Indefinite Listener started...')
        n = 0
        while True:
            if log_indefinite_listening_attempt:
                logger.debug('Indefinite Listening attempt #{}'.format(n + 1))
            try:
                frame = await receiver(websocket, timeout)
                (frame_type, frame_headers, frame_body) = (frame.cmd.lower(),
                    frame.headers, frame.body)
            except trio.TooSlowError as e:
                logger.error('Message receiver timed out\nIndefinite Listening'
                    ' attempt halted')
                break
            except exceptions.EmptyFrameException as e:
                pass
            except exceptions.ErrorFrameReceivedException as e:
                logger.error('Received following error frame \n{}\nIndefinite '
                    'Listening attempt halted!!!\n'.format(traceback.format_exc()))
                receipt_id = utils.get_uuid()
                disconnect_msg = stomp_protocol.disconnect(receipt_id)
                await sender(websocket, disconnect_msg, 0.03)
                await trio.sleep(0.001)
                raise
            except Exception as e:
                logger.error('Receiver failed due to the following error\n{}\n'
                    'Indefinite Listening attempt halted'.format(
                        traceback.format_exc()))
                break
            else:
                if frame_type == 'message':
                    receipt_id = utils.get_uuid()
                    message_id = frame_headers['message-id']
                    nursery.start_soon(processor, websocket, stomp_protocol,
                        timeout, receipt_id, message_id, frame_body, lock, nursery,
                        acknowledgement_receipt_sleep, minor_interval,
                        acknowledgement_receipt_retries, dynamic_queue,
                        acknowledgement_send_retries, frame_headers,
                        deployment_factory, manager, enable_process_life_logging,
                        deployment_poll_interval, deployment_wait_interval,
                        purge_before_subscribe, http_client)
                elif frame_type == 'receipt':
                    receipt_id = frame_headers['receipt-id']
                    stomp_protocol.mark_receipt_read(receipt_id)
            n += 1

    @staticmethod
    async def acknowledge(logger, websocket, ack_msg, timeout, message_id, receipt_id,
            lock, stomp_protocol, receipt_sleep, receipt_retries,
            send_retries):
        acknowledgement_sent = False
        acknowledgement_received_by_server = False

        n = 0
        while not acknowledgement_sent and n < send_retries:
            try:
                if message_id:
                    logger.debug('Acknowledgement message send attempt #{} to '
                        'server for message id: {}'.format(n + 1, message_id))
                await sender(websocket, ack_msg, timeout)
            except trio.TooSlowError as e:
                logger.debug('Acknowledgement message send failed due to timeout')
            else:
                acknowledgement_sent = True
            n += 1

        if acknowledgement_sent:
            if message_id:
                logger.debug('Acknowledgement message sent to server successfully'
                    ' for message id {} after {} attempts'.format(message_id, n))

            try:
                with trio.fail_after(timeout):
                    receipt = stomp_protocol.get_receipt(receipt_id)
                    await receipt.wait()
            except trio.TooSlowError as e:
                if message_id:
                    logger.debug('Acknowledgement receipt for message id {} and '
                        'receipt id {} combo timed out'.format(message_id,
                        receipt_id))
                else:
                    logger.debug('Acknowledgement receipt for receipt id {} combo'
                        ' timed out'.format(receipt_id))
            else:
                acknowledgement_received_by_server = True
                if message_id:
                    logger.debug('Acknowledgement receipt for message id {} and '
                        'receipt id {} GRAND SUCCESS!!'.format(message_id,
                        receipt_id))
                else:
                    logger.debug('Acknowledgement receipt for receipt id {} GRAND'
                        ' SUCCESS!!'.format(receipt_id))

        return acknowledgement_received_by_server
