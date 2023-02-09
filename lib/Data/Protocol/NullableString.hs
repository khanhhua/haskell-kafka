module Data.Protocol.NullableString where

import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int16BE, string8)
import qualified Data.ByteString.Char8 as Char8
import Data.Binary.Get


type NullableString = Maybe String


stringToBuilder :: String -> Builder
stringToBuilder s = int16BE ((fromIntegral . length) s) <> string8 s


nullableStringToBuilder :: NullableString -> Builder
nullableStringToBuilder Nothing = int16BE (-1)
nullableStringToBuilder (Just s) = stringToBuilder s


getString :: Get String
getString = do
  n <- fromIntegral <$> getInt16be
  Char8.unpack <$> getByteString n


getNullableString :: Get NullableString
getNullableString = do
  n <- fromIntegral <$> getInt16be
  if n == -1 then do
    return Nothing
  else do
    Just . Char8.unpack <$> getByteString n
