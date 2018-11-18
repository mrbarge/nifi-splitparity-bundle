# nifi-splitparity-bundle

Splits a Flow File into data shards and accompanying Reed Solomon error
correction parity files that can be used to reassemble the file if 
some of the data shards are lost.

Makes use of Backblaze's Java Reed Solomon implementation, available at:
https://github.com/Backblaze/JavaReedSolomon/tree/master/src

## Status

Experimental, not for production use.

## Parameters

 - dataShards - number of data shards to split the file into
 - parityShards - number of parity shards to generate
 
## TODO

 - dynamic best-guess at dataShards and parityShards based on
   desired dataShared segment size

 - development of a sibling 'mergeparity' processor.
 
## Author

Matt Bargenquast
