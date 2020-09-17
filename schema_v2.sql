-- MySQL dump 10.14  Distrib 5.5.65-MariaDB, for Linux (x86_64)
--
-- Host: iaadasdb001.ite.ti.census.gov    Database: daswiki
-- ------------------------------------------------------
-- Server version	5.7.22-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dvs_dirnames`
--

DROP TABLE IF EXISTS `dvs_dirnames`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_dirnames` (
  `dirnameid` int(11) NOT NULL AUTO_INCREMENT,
  `dirname` varchar(256) NOT NULL,
  PRIMARY KEY (`dirnameid`),
  UNIQUE KEY `dirname` (`dirname`)
) ENGINE=InnoDB AUTO_INCREMENT=175 DEFAULT CHARSET=ascii;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_filenames`
--

DROP TABLE IF EXISTS `dvs_filenames`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_filenames` (
  `filenameid` int(11) NOT NULL AUTO_INCREMENT,
  `filename` varchar(256) NOT NULL,
  PRIMARY KEY (`filenameid`),
  UNIQUE KEY `filename` (`filename`)
) ENGINE=InnoDB AUTO_INCREMENT=175 DEFAULT CHARSET=ascii;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_hashes`
--

DROP TABLE IF EXISTS `dvs_hashes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_hashes` (
  `hashid` int(11) NOT NULL AUTO_INCREMENT,
  `hexhash` varchar(256) NOT NULL,
  `etag` varchar(32) DEFAULT NULL,
  `equivallent_hashid` int(11) DEFAULT NULL,
  PRIMARY KEY (`hashid`),
  UNIQUE KEY `hexhash` (`hexhash`),
  UNIQUE KEY `etag` (`etag`)
) ENGINE=InnoDB AUTO_INCREMENT=306 DEFAULT CHARSET=ascii;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_hashsets`
--

DROP TABLE IF EXISTS `dvs_hashsets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_hashsets` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hashset_hashid` int(11) NOT NULL,
  `hashid` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `hashset_hashid` (`hashset_hashid`,`hashid`),
  KEY `hashid` (`hashid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_hostnames`
--

DROP TABLE IF EXISTS `dvs_hostnames`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_hostnames` (
  `hostid` int(11) NOT NULL AUTO_INCREMENT,
  `hostname` varchar(256) NOT NULL,
  PRIMARY KEY (`hostid`),
  UNIQUE KEY `hostname` (`hostname`)
) ENGINE=InnoDB AUTO_INCREMENT=178 DEFAULT CHARSET=ascii;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_notes`
--

DROP TABLE IF EXISTS `dvs_notes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_notes` (
  `noteid` int(11) NOT NULL AUTO_INCREMENT,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `hashid` int(11) NOT NULL,
  `author` varchar(128) DEFAULT NULL,
  `note` text NOT NULL,
  PRIMARY KEY (`noteid`),
  KEY `hashid` (`hashid`),
  KEY `modified` (`modified`),
  KEY `author` (`author`),
  FULLTEXT KEY `note` (`note`),
  CONSTRAINT `dvs_notes_ibfk_1` FOREIGN KEY (`hashid`) REFERENCES `dvs_hashes` (`hashid`) ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=117 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_objects`
--

DROP TABLE IF EXISTS `dvs_objects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_objects` (
  `objectid` int(11) NOT NULL AUTO_INCREMENT,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `hexhash` varchar(256) NOT NULL,
  `object` json DEFAULT NULL,
  `url` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`objectid`),
  UNIQUE KEY `hexhash` (`hexhash`),
  KEY `created` (`created`),
  KEY `url` (`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `dvs_updates`
--

DROP TABLE IF EXISTS `dvs_updates`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dvs_updates` (
  `updateid` int(11) NOT NULL AUTO_INCREMENT,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `metadata` json DEFAULT NULL,
  `metadata_mtime` int(11) GENERATED ALWAYS AS (json_unquote(json_extract(`metadata`,'$.st_mtime'))) VIRTUAL,
  `hashid` int(11) NOT NULL,
  `hostid` int(11) NOT NULL,
  `dirnameid` int(11) DEFAULT NULL,
  `filenameid` int(11) DEFAULT NULL,
  PRIMARY KEY (`updateid`),
  KEY `t` (`created`),
  KEY `modified` (`modified`),
  KEY `hashid` (`hashid`,`hostid`),
  KEY `hostid` (`hostid`),
  KEY `dirnameid` (`dirnameid`),
  KEY `filenameid` (`filenameid`),
  KEY `metadata_mtime` (`metadata_mtime`),
  CONSTRAINT `dvs_updates_ibfk_1` FOREIGN KEY (`hashid`) REFERENCES `dvs_hashes` (`hashid`),
  CONSTRAINT `dvs_updates_ibfk_2` FOREIGN KEY (`hostid`) REFERENCES `dvs_hostnames` (`hostid`),
  CONSTRAINT `dvs_updates_ibfk_3` FOREIGN KEY (`dirnameid`) REFERENCES `dvs_dirnames` (`dirnameid`),
  CONSTRAINT `dvs_updates_ibfk_4` FOREIGN KEY (`filenameid`) REFERENCES `dvs_filenames` (`filenameid`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2020-09-15 10:52:20
