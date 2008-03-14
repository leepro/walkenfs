-ifndef (WALKENFS_HRL).
-define (WALKENFS_HRL, true).

-record (block_map, { inode, block_no }).       % bag
-record (block_store, { id, data }).            % set
-record (directory_entry, { id, inode }).       % ordered_set
-record (inode, { id, stat }).                  % set
-record (meta, { key, value }).                 % set
-record (symlink, { inode, link }).             % set
-record (xattr, { id, value }).                 % ordered_set

-endif.
