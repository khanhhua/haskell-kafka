module Data.Protocol.NullableByte where
import qualified Data.ByteString as B (ByteString, length, empty)
import Data.Binary.Get (Get, getInt32be, getByteString)
import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int32BE, byteString)

type NullableByte = Maybe B.ByteString


nullableBytesToBuilder :: Maybe B.ByteString -> Builder
nullableBytesToBuilder Nothing = int32BE (-1)
nullableBytesToBuilder (Just bytes) =
  (int32BE . fromIntegral . B.length) bytes
  <> byteString bytes

getNullableBytes :: Get NullableByte
getNullableBytes = do
  n <- fromIntegral <$> getInt32be
  if n == -1 then do
    return Nothing
  else
    Just <$> getByteString n
 