/*
  EECS 484 Project 1
  Galen Gong (ggong)
  Aditi Rajagopal (araja)
*/

INSERT INTO USERS(USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER)
SELECT DISTINCT USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER FROM
PUBLIC_USER_INFORMATION;

INSERT INTO HOME_LOCATION (USER_ID, CITY, STATE, COUNTRY)
SELECT DISTINCT USER_ID, HOMETOWN_CITY, HOMETOWN_STATE, HOMETOWN_COUNTRY FROM 
PUBLIC_USER_INFORMATION;

INSERT INTO CUR_LOCATION (USER_ID, CITY, STATE, COUNTRY)
SELECT USER_ID, CURRENT_CITY, CURRENT_STATE, CURRENT_COUNTRY FROM 
PUBLIC_USER_INFORMATION;

INSERT INTO EDUPROGRAM (USER_ID, INSTITUTION_NAME, PROGRAM_YEAR, PROGRAM_CONCENTRATION, PROGRAM_DEGREE)
SELECT USER_ID, INSTITUTION_NAME, PROGRAM_YEAR, PROGRAM_CONCENTRATION, PROGRAM_DEGREE FROM 
PUBLIC_USER_INFORMATION;

INSERT INTO FRIENDSHIP (USER1_ID, USER2_ID)
SELECT DISTINCT USER1_ID, USER2_ID FROM 
PUBLIC_ARE_FRIENDS
WHERE USER1_ID <> USER2_ID;

INSERT INTO ALBUM (ALBUM_ID, OWNER_ID, ALBUM_NAME, ALBUM_CREATED_TIME,
	ALBUM_MODIFIED_TIME, ALBUM_LINK, ALBUM_VISIBILITY)
SELECT DISTINCT ALBUM_ID, OWNER_ID, ALBUM_NAME, ALBUM_CREATED_TIME,
	ALBUM_MODIFIED_TIME, ALBUM_LINK, ALBUM_VISIBILITY FROM 
PUBLIC_PHOTO_INFORMATION;

INSERT INTO PHOTO (PHOTO_ID, ALBUM_ID, PHOTO_CAPTION, PHOTO_CREATED_TIME, PHOTO_MODIFIED_TIME, PHOTO_LINK)
SELECT DISTINCT PHOTO_ID, ALBUM_ID, PHOTO_CAPTION, PHOTO_CREATED_TIME, PHOTO_MODIFIED_TIME, PHOTO_LINK FROM 
PUBLIC_PHOTO_INFORMATION;

INSERT INTO ALBUM_COVERPHOTO (COVER_PHOTO_ID, ALBUM_ID)
SELECT DISTINCT COVER_PHOTO_ID, ALBUM_ID FROM 
PUBLIC_PHOTO_INFORMATION;

INSERT INTO PHOTOTAGS(PHOTO_ID, TAG_SUBJECT_ID, TAG_X_COORDINATE, TAG_Y_COORDINATE, TAG_CREATED_TIME)
SELECT DISTINCT PHOTO_ID, TAG_SUBJECT_ID, TAG_X_COORDINATE, TAG_Y_COORDINATE, TAG_CREATED_TIME FROM
PUBLIC_TAG_INFORMATION;

INSERT INTO EVENT(EVENT_ID, EVENT_NAME, EVENT_TAGLINE, EVENT_DESCRIPTION, EVENT_HOST,
	EVENT_TYPE, EVENT_SUBTYPE, EVENT_LOCATION, EVENT_CITY, EVENT_STATE, EVENT_COUNTRY, EVENT_START_TIME,
	EVENT_END_TIME, EVENT_CREATOR_ID)
SELECT DISTINCT EVENT_ID, EVENT_NAME, EVENT_TAGLINE, EVENT_DESCRIPTION, EVENT_HOST,
	EVENT_TYPE, EVENT_SUBTYPE, EVENT_LOCATION, EVENT_CITY, EVENT_STATE, EVENT_COUNTRY, EVENT_START_TIME,
	EVENT_END_TIME, EVENT_CREATOR_ID
	FROM
PUBLIC_EVENT_INFORMATION;