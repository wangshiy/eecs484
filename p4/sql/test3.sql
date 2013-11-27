--
-- test insert, SNL join
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

INSERT INTO soaps(soapid, name, network, rating)
	VALUES (1, 'derp', 'd', 2.0);

INSERT INTO soaps(soapid, name, network, rating)
	VALUES (2, 'herp', 'd', 2.0);

SELECT stars.real_name, soaps.name FROM stars, soaps WHERE stars.starid >= soaps.soapid;

DROP TABLE soaps;
DROP TABLE stars;
