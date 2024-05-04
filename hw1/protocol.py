import socket
import time
import queue


class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, payload):
        return self.udp_socket.sendto(payload, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg

    def close(self):
        self.udp_socket.close()


class TcpPacket:
    def __init__(self, chain_num: int, ack_number: int, payload: bytes):
        self.chain_num = chain_num
        self.confirmation_number = ack_number
        self.payload = payload
        self.is_confirmed = False
        self.timestamp_sent = time.time()

    def __len__(self):
        return len(self.payload)

    def __lt__(self, other):
        return self.chain_num < other.chain_num

    def __eq__(self, other):
        return self.chain_num == other.chain_num

    def to_bytes(self) -> bytes:
        chain = self.chain_num.to_bytes(8, "big", signed=False)
        confirm = self.confirmation_number.to_bytes(8, "big", signed=False)
        return chain + confirm + self.payload

    header_length = 16

    def to_packet(payload: bytes) -> 'TcpPacket':
        chain = int.from_bytes(payload[:8], "big", signed=False)
        confirm = int.from_bytes(payload[8:16], "big", signed=False)
        return TcpPacket(chain, confirm, payload[TcpPacket.header_length:])

    def update_t(self, sending_time=None):
        self.timestamp_sent = sending_time if sending_time is not None else time.time()

    confirmation_timeout = 0.1

    @property
    def expired(self):
        return not self.is_confirmed and (time.time() - self.timestamp_sent > self.confirmation_timeout)


class MyTCPProtocol(UDPBasedProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.critical_confirmation_lag = 30
        self.packet_size_maximum = 1500
        self.send_window_size = 18000
        self.send_window = queue.PriorityQueue()
        self.receive_window = queue.PriorityQueue()
        self.rec_buffer = bytes()
        self.total_bytes_sent = 0
        self.total_bytes_confirmed = 0
        self.total_bytes_received = 0

    def send(self, payload: bytes) -> int:
        conf_delay = 0
        total_sent_length = 0
        while (payload or self.total_bytes_confirmed < self.total_bytes_sent) and (
                conf_delay < self.critical_confirmation_lag):
            is_window_full = (self.total_bytes_sent - self.total_bytes_confirmed > self.send_window_size)
            if not is_window_full and payload:
                packet_end = min(self.packet_size_maximum, len(payload))
                packet_length = self.sendPacket(TcpPacket(self.total_bytes_sent,
                                                          self.total_bytes_received,
                                                          payload[: packet_end]))
                payload = payload[packet_length:]
                total_sent_length += packet_length
                self.receiveAndProcessPacket(0.)
            else:
                if self.receiveAndProcessPacket(TcpPacket.confirmation_timeout):
                    conf_delay = 0
                else:
                    conf_delay += 1
            self.recentPrevPackage()
        return total_sent_length

    def recv(self, n: int) -> bytes:
        packet_end = min(n, len(self.rec_buffer))
        payload = self.rec_buffer[:packet_end]
        self.rec_buffer = self.rec_buffer[packet_end:]
        while len(payload) < n:
            self.receiveAndProcessPacket()
            packet_end = min(n, len(self.rec_buffer))
            payload += self.rec_buffer[:packet_end]
            self.rec_buffer = self.rec_buffer[packet_end:]
        return payload

    def close(self):
        super().close()

    def receiveAndProcessPacket(self, timeout: float = None) -> bool:
        self.udp_socket.settimeout(timeout)
        try:
            packet = TcpPacket.to_packet(self.recvfrom(self.packet_size_maximum + TcpPacket.header_length))
        except socket.error:
            return False

        if len(packet):
            self.receive_window.put((packet.chain_num, packet), block=False)
            self.adjustRecvWindow()

        if packet.confirmation_number > self.total_bytes_confirmed:
            self.total_bytes_confirmed = packet.confirmation_number
            self.adjustSendWindow()

        return True

    def sendPacket(self, packet: TcpPacket) -> int:
        self.udp_socket.settimeout(None)
        just_sent = self.sendto(packet.to_bytes()) - packet.header_length

        if packet.chain_num == self.total_bytes_sent:
            self.total_bytes_sent += just_sent
        elif packet.chain_num > self.total_bytes_sent:
            raise ValueError(f'{packet.chain_num} -> this chain num is illegal and too big ')

        if len(packet):
            packet.payload = packet.payload[: just_sent]
            packet.update_t()
            self.send_window.put((packet.chain_num, packet), block=False)

        return just_sent

    def recentPrevPackage(self, force=False):
        if self.send_window.empty():
            return
        _, prev_package = self.send_window.get(block=False)
        if prev_package.expired or force:
            self.sendPacket(prev_package)
        else:
            self.send_window.put((prev_package.chain_num, prev_package), block=False)

    def adjustRecvWindow(self):
        prev_package = None
        while not self.receive_window.empty():
            _, prev_package = self.receive_window.get(block=False)
            if prev_package.chain_num < self.total_bytes_received:
                prev_package.is_confirmed = True
            elif prev_package.chain_num == self.total_bytes_received:
                self.rec_buffer += prev_package.payload
                self.total_bytes_received += len(prev_package)
                prev_package.is_confirmed = True
            else:
                self.receive_window.put((prev_package.chain_num, prev_package), block=False)
                break

        if prev_package is not None:
            self.sendPacket(TcpPacket(self.total_bytes_sent, self.total_bytes_received, bytes()))

    def adjustSendWindow(self):
        while not self.send_window.empty():
            _, prev_package = self.send_window.get(block=False)
            if prev_package.chain_num >= self.total_bytes_confirmed:
                self.send_window.put((prev_package.chain_num, prev_package), block=False)
                break
