--
-- test insert, indexed filescan
--

-- create relations
CREATE TABLE soaps(soapid integer, name char(32), 
                   network char(4), rating double);
CREATE TABLE stars(starid integer, real_name char(20), 
                   plays char(12), soapid integer);

INSERT INTO stars(starid, real_name, plays, soapid) 
	VALUES (100, 'Posey, Parker', 'Tess', 6);

INSERT INTO stars (real_name, soapid, starid, plays) 
	VALUES ('Bonarrigo, Laura', 3, 101, 'Cassie');

CREATE INDEX stars (soapid);

SELECT stars.soapid FROM stars WHERE stars.soapid = 6;
SELECT stars.soapid FROM stars WHERE stars.soapid >= 3;

DROP TABLE soaps;
DROP TABLE stars;

