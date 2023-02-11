module Client.Networking where

import Network.Socket
  ( HostName
  , ServiceName
  , AddrInfoFlag (AI_ADDRCONFIG)
  , Socket
  , defaultHints
  , getAddrInfo
  , getSocketOption
  , AddrInfo (addrFlags, addrFamily, addrProtocol, addrAddress, addrSocketType), Family (AF_INET), connect, SocketOption (SoError, NoDelay, KeepAlive), openSocket, SocketType (Stream), setSocketOption
  )
import qualified Network.Socket.ByteString as NS (recv)
import qualified Network.Socket.ByteString.Lazy as NSBL (sendAll)
import qualified Data.ByteString as B (ByteString)
import qualified Data.ByteString.Lazy as BL (length)

import Data.Protocol.Classes (KafkaRequest)
import Data.Protocol.MessageHeader (CorrelationId)
import Data.Protocol (encodeMessage)

type BootstrapUrl = (HostName, ServiceName)


dEFAULT_BUFFER_SIZE = 32767 :: Int

openTcpSocket :: HostName -> ServiceName -> IO Socket
openTcpSocket hostname port = do
  -- https://hackage.haskell.org/package/network-simple-0.4.5/docs/src/Network.Simple.TCP.html#connectSock
  addrInfo <- head <$> getAddrInfo (Just hints) (Just hostname) (Just port)
  sock <- openSocket addrInfo
  setSocketOption sock NoDelay 1
  setSocketOption sock KeepAlive 1
  connect sock (addrAddress addrInfo)
  -- TODO sClose
  return sock
  where
    hints = defaultHints
      { addrFamily = AF_INET
      , addrFlags = [AI_ADDRCONFIG]
      , addrSocketType = Stream
      }


tryOpenTcpSocket :: [BootstrapUrl] -> IO (Maybe Socket)
tryOpenTcpSocket [] = return Nothing
tryOpenTcpSocket ((hostname, port) : rest) = do
  sock <- openTcpSocket hostname port
  sockOpt <- getSocketOption sock SoError
  if sockOpt == 0 then
    return $ Just sock
  else
    tryOpenTcpSocket rest


sendAndRecv :: KafkaRequest a => Socket -> a -> CorrelationId -> IO B.ByteString
sendAndRecv sock req correlationId = do
  let
    bytes = encodeMessage req correlationId
  print $ "Sending " <> show (BL.length bytes) <> " bytes"
  -- print bytes
  NSBL.sendAll sock bytes
  NS.recv sock dEFAULT_BUFFER_SIZE