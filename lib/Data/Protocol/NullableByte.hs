module Data.Protocol.NullableByte where
import Data.ByteString (ByteString, empty)
import Data.Binary.Get (Get, getInt32be, getByteString)

type NullableByte = ByteString


getNullableBytes :: Get NullableByte
getNullableBytes = do
  n <- fromIntegral <$> getInt32be
  if n == -1 then do
    return empty
  else
    getByteString n
 