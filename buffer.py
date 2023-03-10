import socket

class Buffer(object):
    def __init__(self):
        self.data = bytearray(b'')
        # self.__maxsz = 64 * 1024
        self.__maxsz = 2048000

    def read_from_sock(self, sock):
        # WARNING: should change /proc/sys/net/wmem and /proc/sys/net/rmem
        ntot = 0
        try:
            while ntot < self.__maxsz:
                new_data = sock.recv(self.__maxsz - ntot)
                nread = len(new_data)
                ntot += nread
                print('[Buffer.read_from_sock] nread = {}, ntot = {}'.format(nread, ntot))
                if nread == 0:
                    # peer close
                    return 0
                else:
                    self.data.extend(new_data)
            return ntot
        except BlockingIOError as bio_e:
            print('[Buffer.read_from_sock] BlockingIOError: {}'.format(bio_e))
            ntot = -1 if ntot == 0 else ntot
            return ntot
        except OSError as e:
            raise

    def retrive(self, nsize):
        nsize = nsize if nsize >= 0 else len(self.data)
        del self.data[:nsize]

    def retrive_as_bytes(self, nsize):
        nsize = nsize if nsize >= 0 else len(self.data)
        ret_data = self.data[:nsize]
        del self.data[:nsize]
        return bytearray(ret_data)

    def readable_bytes(self):
        return len(self.data)

    def append(self, data):
        self.data.extend(data)
