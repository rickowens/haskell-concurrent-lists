{- |
  This module provides utilities for easily performing current operations.
  Specifically, it provides a way to operate on lists of IO actions much
  like `sequence` and `mapM`, but in such a way where the actions are all
  performed concurrently. All other semantics are the same: exceptions
  encountered concurrently will be thrown in the calling thread (but
  possibly not at the same time you would expect if you were to use
  `sequence`, obviously), functions in this module block the calling
  thread until all of the concurrent threads have returned, and the
  order of the return values are the same as in their counterparts.
-}
module Control.Concurrent.ConcurrentLists (
  concurrently,
  concurrently_,
  concurrentN,
  concurrentN_,
  cMapM,
  cMapM_,
  cMapMN,
  cMapMN_
) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (MVar, newEmptyMVar, takeMVar, putMVar)
import Control.Exception (try, throw, SomeException)
import Control.Monad (void)

-- Public Types ---------------------------------------------------------------
-- Semi-Public Types ----------------------------------------------------------
-- Public Functions -----------------------------------------------------------


{- |
  Like `sequence`, but all io operations are executed concurrently.
-}
concurrently :: [IO a] -> IO [a]
concurrently ios = do
  jobs <- mapM async ios
  results <- mapM takeMVar jobs
  case sequence results of
    Right vals -> return vals
    Left err -> throw err


{- |
  Like `sequence_`, but all io operations are executed concurrently.
-}
concurrently_ :: [IO a] -> IO ()
concurrently_ = void . concurrently


{- |
  Like `concurrently`, but use at most N threads.
-}
concurrentN :: Int -> [IO a] -> IO [a]
concurrentN n ios = do
  chan <- newEmptyMVar
  mapM_ (forkIO . const (handler chan)) [1 .. n]
  packages <- mapM package ios
  mapM_ (putMVar chan . Just) packages
  putMVar chan Nothing -- send the kill signal
  results <- mapM (takeMVar . snd) packages
  case sequence results of
    Right vals -> return vals
    Left err -> throw (err :: SomeException)
  where
    package io = do
      response <- newEmptyMVar
      return (io, response)

    handler chan = do
      job <- takeMVar chan
      case job of
        Nothing ->
          -- put back the end marker and terminate
          putMVar chan Nothing
        Just (io, response) -> do
          result <- try io
          putMVar response result
          -- loop until we get `Nothing`
          handler chan


{- |
  Like `concurrently_`, but use at most N threads.
-}
concurrentN_ :: Int -> [IO a] -> IO ()
concurrentN_ n =
  -- we could probably be more memory efficient here.
  void . concurrentN n


{- |
  Like `mapM`, but all io operations are executed concurrently.
-}
cMapM :: (a -> IO b) -> [a] -> IO [b]
cMapM f = concurrently . map f


{- |
  Like `mapM_`, but all io operations are executed concurrently.
-}
cMapM_ :: (a -> IO b) -> [a] -> IO ()
cMapM_ f = concurrently_ . map f


{- |
  Like `cMapM`, but use at most N threads.
-}
cMapMN :: Int -> (a -> IO b) -> [a] -> IO [b]
cMapMN n f = concurrentN n . map f


{- |
  Like `cMapM_`, but use at most N threads.
-}
cMapMN_ :: Int -> (a -> IO b) -> [a] -> IO ()
cMapMN_ n f = concurrentN_ n . map f


-- Private Types --------------------------------------------------------------
-- Private Functions ----------------------------------------------------------

{- |
  Kicks off an IO in a new thread
-}
async :: IO a -> IO (MVar (Either SomeException a))
async io = do
  job <- newEmptyMVar
  forkIO $ do
    result <- try io
    putMVar job result
  return job


-- Tests ----------------------------------------------------------------------
