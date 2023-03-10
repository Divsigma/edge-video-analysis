import asyncio

class CloudServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def eof_received(self):
        print('Closing: {}'.format(self.transport))
        self.transport.write('server: done writing'.encode())
        self.transport.close()

    def data_received(self, data):
        message = data.decode()
        print('Data received: {!r} on {}'.format(message, self.transport))

        print('Send: {!r}'.format(message))
        self.transport.write(data)

async def main():
    # Get a reference to the event loop as we plan to use
    # low-level APIs.
    loop = asyncio.get_running_loop()

    server = await loop.create_server(
        lambda: CloudServerProtocol(),
        '0.0.0.0', 9999)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
