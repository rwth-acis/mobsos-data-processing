
CREATE DATABASE `LAS2PEERMON` IF NOT EXISTS  /*!40100 DEFAULT CHARACTER SET utf8 */;
USE `LAS2PEERMON`;


CREATE TABLE MESSAGE IF NOT EXISTS 
(
  ID INT NOT NULL
  auto_increment,
	EVENT VARCHAR
  (255) NOT NULL,
	TIME_STAMP TIMESTAMP NOT NULL,
	SOURCE_NODE VARCHAR
  (255),
	SOURCE_AGENT CHAR
  (32),
	DESTINATION_NODE VARCHAR
  (255),
	DESTINATION_AGENT CHAR
  (32),
	REMARKS json DEFAULT NULL,
	CONSTRAINT MESSAGE_PK PRIMARY KEY
  (ID)
);


  CREATE TABLE AGENT IF NOT EXISTS 
  (
    AGENT_ID CHAR(32) NOT NULL,
    TYPE ENUM("USER",
    "SERVICE",
    "GROUP",
    "MONITORING",
    "MEDIATOR"
  )
  ,
	CONSTRAINT AGENT_PK PRIMARY KEY
  (AGENT_ID)
);


  CREATE TABLE NODE IF NOT EXISTS 
  (
    NODE_ID VARCHAR(255) NOT NULL,
    NODE_LOCATION VARCHAR(255) NOT NULL,
    CONSTRAINT NODE_PK PRIMARY KEY (NODE_ID)
  );


  CREATE TABLE REGISTERED_AT IF NOT EXISTS 
  (
    REGISTRATION_DATE TIMESTAMP NULL,
    UNREGISTRATION_DATE TIMESTAMP NULL,
    AGENT_ID CHAR(32) NOT NULL,
    RUNNING_AT VARCHAR(255),
    CONSTRAINT AGENT_ID_FK FOREIGN KEY (AGENT_ID) REFERENCES AGENT(AGENT_ID),
    CONSTRAINT RUNNING_AT_FK FOREIGN KEY (RUNNING_AT) REFERENCES NODE(NODE_ID)
  );


  CREATE TABLE SERVICE IF NOT EXISTS 
  (
    AGENT_ID CHAR(32) NOT NULL,
    SERVICE_CLASS_NAME VARCHAR(255),
    SERVICE_PATH VARCHAR(255) NULL,
    CONSTRAINT SERVICE_PK PRIMARY KEY (AGENT_ID),
    CONSTRAINT SERVICE_FK FOREIGN KEY (AGENT_ID) REFERENCES AGENT(AGENT_ID),
    CONSTRAINT SERVICE_UK UNIQUE
    KEY
    (AGENT_ID,SERVICE_CLASS_NAME)
);

    CREATE TABLE GROUP_INFORMATION IF NOT EXISTS 
    (
      GROUP_AGENT_ID_MD5 CHAR(32) NOT NULL,
      GROUP_AGENT_ID CHAR(128) NOT NULL,
      GROUP_NAME VARCHAR(255) NOT NULL,
      PUBLIC BOOLEAN,
    CONSTRAINT GROUP_AGENT_ID_FK FOREIGN KEY
      (GROUP_AGENT_ID_MD5) REFERENCES AGENT
      (AGENT_ID)
);
