import base64

from Crypto import Random
from Crypto.Cipher import AES

ENCODING = 'utf-8'

class AesCipher():
    def __init__(self, key):
        self.block_size = 32
        self.key = key

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key.encode(ENCODING), AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode(ENCODING)

    def _pad(self, s):
        return s + (self.block_size - len(s) % self.block_size) * chr(self.block_size - len(s) % self.block_size)

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s) - 1:])]
