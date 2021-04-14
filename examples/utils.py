import lz4
import msgpack


def pack(data):
    msgp = msgpack.packb(data)
    packed = lz4.frame.compress(msgp, compression_level=16, block_size=lz4.frame.BLOCKSIZE_MAX4MB)
    return packed


def unpack(self, data):
    decomp = lz4.frame.decompress(data)
    unpacked = msgpack.unpackb(decomp)
    return unpacked
