# girderfs
FUSE filesystem allowing to directly mount resources from Girder's fs assetstore

Requires running Girder with recent version of [ythub plugin](https://github.com/data-exp-lab/girder_ythub) (for 
``GET /folder/{id}/listing`` and ``GET /item/%s/listing``)
