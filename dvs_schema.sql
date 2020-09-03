CREATE TABLE `dvs_hashes` (
    `hashid` int(11) NOT NULL AUTO_INCREMENT,
    `hexhash` varchar(256) NOT NULL,
    PRIMARY KEY (`hashid`),
    index  (hexhash)
) ENGINE=InnoDB CHARSET=ascii;
/*!40101 SET character_set_client = @saved_cs_client */;

CREATE TABLE dvs_hashsets (
  id int not null auto_increment,
  hashset_hashid int not null,
  hashid int not null,
  primary key (id),
  index (hashset_hashid,hashid),
  index (hashid)
) engine=innodb;


CREATE TABLE dvs_notes (
  noteid int not null auto_increment,
  t timestamp not null default current_timestamp,
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

create table dvs_actions (
  actionid int not null auto_increment,
  t timestamp not null default current_timestamp,
  modified timestamp not null default current_timestamp on update current_timestamp,
  action JSON,
  hashid int not null,
  primary key (actionid),
  index (t),
  index (modified),
  foreign key (hashid) references dvs_hashes(hashid) on update cascade
) engine=innodb;

create table dvs_hosts (
  hostid int not null auto_increment,
  hostname varchar(256) not null,
  primary key (hostid),
  index (hostname)
) engine=innodb charset=ascii;

create table dvs_files (
  fileid int not null auto_increment,
  t timestamp not null default current_timestamp,
  modified timestamp not null default current_timestamp on update current_timestamp,
  hostid int not null,
  dirname varchar(4096),
  filename varchar(4096),
  metadata JSON,
  metadata_mtime int as (JSON_UNQUOTE(metadata->"$.st_mtime")),
  hashid int,
  primary key (fileid),
  index (t),
  index (modified),
  index (pathname),
  index (hashid),
  index (metadata_mtime),
  foreign key (hashid) references dvs_hashes(hashid) on update cascade
) engine=innodb charset=utf-8;
