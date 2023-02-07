module Data.Protocol.NullableString where
import Data.Binary.Builder (Builder)
import Data.ByteString.Builder (int16BE, string8)


type NullableString = Maybe String


stringToBuilder :: String -> Builder
stringToBuilder s = int16BE ((fromIntegral . length) s) <> string8 s


nullableStringToBuilder :: NullableString -> Builder
nullableStringToBuilder Nothing = int16BE (-1)
nullableStringToBuilder (Just s) = stringToBuilder s
