PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE `follows` (
  `f1` INTEGER NOT NULL,
  `f2` INTEGER NOT NULL,
  PRIMARY KEY (`f1`,`f2`),
  FOREIGN KEY (`f1`) REFERENCES `user_profiles`(`uid`),
  FOREIGN KEY (`f2`) REFERENCES `user_profiles`(`uid`)
);

INSERT INTO follows VALUES(1,2);
INSERT INTO follows VALUES(2,3);
INSERT INTO follows VALUES(7,1);
INSERT INTO follows VALUES(1,3);
INSERT INTO follows VALUES(3,6);
INSERT INTO follows VALUES(5,6);
INSERT INTO follows VALUES(5,1);
INSERT INTO follows VALUES(1,5);
INSERT INTO follows VALUES(3,4);

CREATE TABLE `tweets` (
  `id` BIGINT NOT NULL,
  `uid` INTEGER NOT NULL,
  `text` char(140) NOT NULL,
  `createdate` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  FOREIGN KEY (`uid`) REFERENCES `user_profiles`(`uid`)
);

INSERT INTO tweets VALUES(1,1,'Hello I''m Tony Stark.','2018-03-09 20:00:14');
INSERT INTO tweets VALUES(2,3,'Today I have went shopping at Laffayette.','2018-03-09 20:01:02');
INSERT INTO tweets VALUES(4,1,'My company is hiring interns.','2018-03-12 02:30:30');
INSERT INTO tweets VALUES(5,4,'Oh, I only have a few fans.','2018-03-12 02:30:30');
INSERT INTO tweets VALUES(6,6,'Let’s celebrate women, fight for equality and support each other,  not just today, but everyday!','2018-03-12 02:30:30');
INSERT INTO tweets VALUES(7,5,'I love my new boyfriend.','2018-03-12 02:30:30');
INSERT INTO tweets VALUES(8,4,'I feel tired and don''t want to write SQLs.','2018-03-12 02:30:30');
INSERT INTO tweets VALUES(9,6,'I had a pretty rough time during the last couple of weeks','2018-03-12 02:30:30');
CREATE TABLE IF NOT EXISTS "user_profiles" (`uid` INTEGER NOT NULL, `name` varchar(255) DEFAULT NULL, `email` varchar(255) DEFAULT NULL, `partitionid` INTEGER DEFAULT NULL, `followers` INTEGER DEFAULT NULL, PRIMARY KEY (`uid`));
INSERT INTO user_profiles VALUES(1,'Iron Man','ts@richest.com',1,6662425);
INSERT INTO user_profiles VALUES(2,'Mary','Mary@yale.edu',2,890);
INSERT INTO user_profiles VALUES(3,replace('Black Widow\n','\n',char(10)),'bw@superhero.com',23,35425845);
INSERT INTO user_profiles VALUES(4,'Susan',replace('susan@gmail.com\n','\n',char(10)),1,100);
INSERT INTO user_profiles VALUES(5,'Tyler Swift',replace('ts@superstar.com\n','\n',char(10)),10,99999998);
INSERT INTO user_profiles VALUES(6,'Celine Dion','cd@superstar.com',5,23927492);
INSERT INTO user_profiles VALUES(7,'Natalie Portman','np@superstar.com',5,94375428);
COMMIT;
