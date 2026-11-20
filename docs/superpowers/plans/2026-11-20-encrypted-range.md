# Encrypted byte ranges

For a nonempty S3 Range, fetch the covering complete AES-GCM frames from the version's selected Kubo node using one half-open ciphertext range. Compute the last frame's size from the object's plaintext length. Authenticate each returned frame before exposing its plaintext, trim the two boundaries, and reject truncation. Keep the existing ciphertext layout, metadata, ETag, and HTTP range behavior.

SSE-S3 and fingerprinted SSE-C objects need no preliminary body read. For legacy SSE-C objects without a fingerprint, preserve the existing one-time full authentication and key claim before the bounded response read. Full GET and HEAD keep their authentication contracts.

Exercise signed SSE-S3/SSE-C reads including a cross-frame range, a short final frame, incorrect keys and corrupt selected data. Inspect Kubo offsets and lengths as well as response bytes; unselected ciphertext is not authenticated by a normal range read.
