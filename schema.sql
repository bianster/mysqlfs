-- MySQL dump 10.10
--
-- Host: localhost    Database: mysqlfs
-- ------------------------------------------------------
-- Server version	5.0.22-Debian_0ubuntu6.06.2-log

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
-- Table structure for table `data`
--

DROP TABLE IF EXISTS `data`;
CREATE TABLE `data` (
  `inode` bigint(20) NOT NULL,
  `data` longblob NOT NULL,
  PRIMARY KEY  (`inode`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `data`
--


/*!40000 ALTER TABLE `data` DISABLE KEYS */;
LOCK TABLES `data` WRITE;
UNLOCK TABLES;
/*!40000 ALTER TABLE `data` ENABLE KEYS */;

--
-- Table structure for table `inodes`
--

DROP TABLE IF EXISTS `inodes`;
CREATE TABLE `inodes` (
  `inode` bigint(20) NOT NULL,
  `inuse` int(11) NOT NULL default '0',
  `deleted` tinyint(4) NOT NULL default '0',
  `mode` int(11) default NULL,
  `uid` int(10) unsigned NOT NULL default '0',
  `gid` int(10) unsigned NOT NULL default '0',
  `atime` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `mtime` timestamp NOT NULL default '0000-00-00 00:00:00',
  `ctime` timestamp NOT NULL default '0000-00-00 00:00:00',
  `size` bigint(20) NOT NULL default '0',
  PRIMARY KEY  (`inode`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `inodes`
--


/*!40000 ALTER TABLE `inodes` DISABLE KEYS */;
LOCK TABLES `inodes` WRITE;
INSERT INTO `inodes` VALUES (1,0,0,16895,0,0,'2006-09-12 05:25:03','0000-00-00 00:00:00','0000-00-00 00:00:00',0);
UNLOCK TABLES;
/*!40000 ALTER TABLE `inodes` ENABLE KEYS */;

/*!50003 SET @OLD_SQL_MODE=@@SQL_MODE*/;
DELIMITER ;;
/*!50003 SET SESSION SQL_MODE="" */;;
/*!50003 CREATE */ /*!50017 DEFINER=`root`@`localhost` */ /*!50003 TRIGGER `drop_data` AFTER DELETE ON `inodes` FOR EACH ROW BEGIN DELETE FROM data WHERE inode=OLD.inode; END */;;

DELIMITER ;
/*!50003 SET SESSION SQL_MODE=@OLD_SQL_MODE */;

--
-- Table structure for table `tree`
--

DROP TABLE IF EXISTS `tree`;
CREATE TABLE `tree` (
  `inode` int(10) unsigned NOT NULL auto_increment,
  `parent` int(10) unsigned default NULL,
  `name` varchar(255) NOT NULL,
  UNIQUE KEY `name` (`name`,`parent`),
  KEY `inode` (`inode`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Dumping data for table `tree`
--


/*!40000 ALTER TABLE `tree` DISABLE KEYS */;
LOCK TABLES `tree` WRITE;
INSERT INTO `tree` VALUES (1,NULL,'/');
UNLOCK TABLES;
/*!40000 ALTER TABLE `tree` ENABLE KEYS */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

