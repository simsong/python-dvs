drop table if exists dvs_hashes;
CREATE TABLE `dvs_hashes` (
    `hashid` int(11) NOT NULL AUTO_INCREMENT,
    `hexhash` varchar(256) NOT NULL,
    etag varchar(32),
     equivallent_hashid int (11),
    PRIMARY KEY (`hashid`),
    unique index (etag),
    unique index  (hexhash)
) ENGINE=InnoDB CHARSET=ascii;

create table dvs_hostnames (
  hostid int not null auto_increment,
  hostname varchar(256) not null,
  primary key (hostid),
  unique index (hostname)
) engine=innodb charset=ascii;

create table dvs_dirnames (
  dirnameid int not null auto_increment,
  dirname varchar(256) not null,
  primary key (dirnameid),
  unique index (dirname)
) engine=innodb charset=ascii;

create table dvs_filenames (
  filenameid int not null auto_increment,
  filename varchar(256) not null,
  primary key (filenameid),
  unique index (filename)
) engine=innodb charset=ascii;

CREATE TABLE dvs_hashsets (
  id int not null auto_increment,
  hashset_hashid int not null,
  hashid int not null,
  primary key (id),
  unique index (hashset_hashid,hashid),
  index (hashid)
) engine=innodb;


CREATE TABLE dvs_notes (
  noteid int not null auto_increment,
  created timestamp not null default current_timestamp,
  modified timestamp not null default current_timestamp on update current_timestamp,
  hashid int not null,
  author varchar(128),
  note text not null,
  primary key (noteid),
  foreign key (hashid) references dvs_hashes(hashid) on update cascade,
  index (modified),
  index (author),
  fulltext index (note)
) engine=innodb charset=utf8;

create table dvs_updates (
  updateid int not null auto_increment,
  created timestamp not null default current_timestamp,
  modified timestamp not null default current_timestamp on update current_timestamp,
  metadata JSON,
  metadata_mtime int as (JSON_UNQUOTE(metadata->"$.st_mtime")),
  hashid int not null,
  hostid int not null,
  dirnameid int,
  filenameid int,
  primary key (updateid),
  index (t),
  index (modified),
  index (metadata_mtime),
  index (hashid,hostid),
  foreign key (hashid) references dvs_hashes(hashid),
  foreign key (hostid) references dvs_hostnames(hostid),
  foreign key (dirnameid) references dvs_dirnames(dirnameid),
  foreign key (filenameid) references dvs_filenames(filenameid)
) engine=innodb;

