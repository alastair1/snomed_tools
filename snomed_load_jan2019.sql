/* snomed_load_jan2019.sql
 *
 * Load SNOMED CT International Edition RF2 distribution files into MySQL database
 *
 * MySQL Workbench doesn't like 'load data local' operations so use shell 'mysql --password --local-infile --database=snomed_jan2019' instead and copy/paste
 *
 */

create database if not exists snomed_jan2019 default collate latin1_general_cs;

use snomed_jan2019;

show collation;

set @@global.local_infile="ON";
show variables like "local_infile";

/* Drop tables */

SELECT TABLE_NAME,
	COLUMN_NAME,
	CONSTRAINT_NAME, 
	REFERENCED_TABLE_NAME,
	REFERENCED_COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE lower(REFERENCED_TABLE_NAME) = 'concept_load';

/* Mappings */

drop table if exists map_load;

create table map_load (
	id char(36) not null primary key,
	effective_time date not null,
	active smallint not null /* seems to be constant = 1 */,
	module_id bigint not null /* seems to be constant = 900000000000207008 for SNOMED CT core module */,     
	refset_id bigint not null, 
	referenced_component_id bigint not null,
	map_target varchar(20) not null
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Refset/Map/der2_sRefset_SimpleMapSnapshot_INT_20190131.txt'
into table map_load
lines terminated by '\r\n'
ignore 1 lines;

select refset_id,count(*) from map_load group by refset_id;

select count(*) from map_load where refset_id = 446608001 /* icd-O */;
select count(*) from map_load where refset_id = 900000000000497000 /* CTV3 */;

drop table if exists ctv3_snomed_map;

create table ctv3_snomed_map as
select referenced_component_id as concept_id, replace(map_target, char(13), '') as ctv3_code
from map_load
where refset_id = 900000000000497000 /* CTV3 */;

alter table ctv3_snomed_map add primary key (concept_id);
alter table ctv3_snomed_map add unique (ctv3_code);

drop table if exists extended_map_load;

create table extended_map_load (
	id char(36) not null primary key,
	effective_time date not null,
	active smallint not null /* 0, 1 */,
	module_id bigint not null /* seems to be constant = 900000000000207008 for SNOMED CT core module */,     
	refset_id bigint not null /* seems to be constant 447562003 for ICD-10 */, 
	referenced_component_id bigint not null,
	map_group smallint not null /* 1, 2, 3, 4, 5 etc */,
	map_priority smallint /* 1, 2, 3 etc */,
	map_rule varchar(500) not null /* eg TRUE */,
	map_advice varchar(500),
	map_target varchar(20) not null,
	correlation_id bigint /* constant 447561005 */,
	map_category_id bigint /* Child of ICD-10 map category value concept - 70% are: Map source concept is properly classified */
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Refset/Map/der2_iisssccRefset_ExtendedMapSnapshot_INT_20190131.txt'
into table extended_map_load
lines terminated by '\r\n'
ignore 1 lines;

/* Concepts */

drop table if exists concept_load;

create table concept_load (
	concept_id bigint not null primary key,
	effective_time date not null,
	active smallint not null,
	module_id bigint not null,
	definition_status_id bigint not null
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Terminology/sct2_Concept_Snapshot_INT_20190131.txt'
into table concept_load
lines terminated by '\r\n'
ignore 1 lines;

drop table if exists description_load;

create table description_load (
	description_id bigint not null primary key,
	effective_time date not null,
	active smallint not null,
	module_id bigint not null,
	concept_id bigint not null,
	language_code char(2) not null,
	type_id bigint not null,
	term text not null,
	case_significance_id bigint not null
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Terminology/sct2_Description_Snapshot-en_INT_20190131.txt'
into table description_load
lines terminated by '\r\n'
ignore 1 lines;

drop table if exists relationship_load;

create table relationship_load (
	relationship_id bigint not null primary key,
	effective_time date not null,
	active smallint not null,
	module_id bigint not null,
	source_id bigint not null,
	destination_id bigint not null,
	relationship_group smallint not null /* 66 values */,
	type_id bigint not null,
	characteristic_type_id bigint not null /* status versus derived */,
	modifier_id bigint not null /* constant */
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Terminology/sct2_Relationship_Snapshot_INT_20190131.txt'
into table relationship_load
lines terminated by '\r\n'
ignore 1 lines;

drop table if exists relationship;

create table relationship as
select relationship_id, effective_time,source_id, destination_id, relationship_group, type_id,
case when characteristic_type_id = 900000000000011006 then 1 else 0 end as stated
from relationship_load
where active;

alter table relationship add primary key (relationship_id);
/*
drop table if exists stated_relationship_load;

create table stated_relationship_load (
	relationship_id bigint not null primary key,
	effective_time date not null,
	active smallint not null,
	module_id bigint not null, -- 2 values: "core module" versus "model component module"
	source_id bigint not null,
	destination_id bigint not null,
	relationship_group smallint not null, -- 0-7
	type_id bigint not null, -- 58 values
	characteristic_type_id bigint not null, -- constant: "stated defining relationship"
	modifier_id bigint not null -- constant
);

load data local
infile '/users/alastair/Downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Terminology/sct2_StatedRelationship_Snapshot_INT_20190131.txt'
into table stated_relationship_load
lines terminated by '\r\n'
ignore 1 lines;

drop table if exists stated_relationship;

create table stated_relationship as
select relationship_id, effective_time, source_id, destination_id, relationship_group, type_id
from stated_relationship_load
where active;

alter table stated_relationship add primary key (relationship_id);
alter table stated_relationship add unique (source_id, destination_id, relationship_group,type_id);
*/
/* Preferred terms */

drop table if exists en_lang_refset;

create table en_lang_refset (
	member_id char(36) not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	refset_id bigint not null,
	description_id bigint not null,
	acceptability_id bigint not null,
	primary key (member_id)
);

load data local
infile '/users/alastair/downloads/SnomedCT_InternationalRF2_MEMBER_20190131T120000Z/Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-en_INT_20190131.txt'
into table en_lang_refset
lines terminated by '\r\n'
ignore 1 lines;

drop table if exists gb_en_lang_refset_active;

create table gb_en_lang_refset_active as
select description_id, effective_date, acceptability_id -- FIX might not need effective_date column
from en_lang_refset
	natural join (select description_id from description_load) a -- exclude text definitions
where refset_id = 900000000000508004 -- GB English (from core module and model component module)
	and active = 1;

alter table gb_en_lang_refset_active add primary key (description_id);

drop table if exists preferred_term_description_id;

create table preferred_term_description_id as
select description_id
from gb_en_lang_refset_active
where acceptability_id = 900000000000548007; -- preferred term

alter table preferred_term_description_id add primary key (description_id);

drop table if exists synonym_description_id;

create table synonym_description_id as
select description_id
from gb_en_lang_refset_active
where acceptability_id = 900000000000549004; -- synonym

alter table synonym_description_id add primary key (description_id);

create table gb_en_description as
select description_id, concept_id, type_id, term, case_significance_id, acceptability_id
from description_load
natural join (select description_id, acceptability_id from gb_en_lang_refset_active) a;

alter table gb_en_description add primary key (description_id);
alter table gb_en_description add foreign key (concept_id) references concept_load (concept_id);

drop table if exists preferred_term;

create table preferred_term (
	concept_id bigint not null unique,
	preferred_term_description_id bigint not null primary key,
	preferred_term text not null,
	preferred_term_case_significant smallint not null
);

insert into preferred_term (concept_id, preferred_term_description_id, preferred_term, preferred_term_case_significant)
select concept_id,
description_id as preferred_term_description_id,
term as preferred_term,
case when case_significance_id = 900000000000017005 then 1 else 0 end as preferred_term_case_significant
from preferred_term_description_id
natural join description_load
where active = 1
and language_code = 'en'
and type_id = 900000000000013009; -- synonym

drop table if exists synonym_term;

create table synonym_term (
	concept_id bigint not null,
	synonym_description_id bigint not null primary key,
	synonym_term text not null,
	synonym_case_significant smallint not null
);

insert into synonym_term (concept_id, synonym_description_id, synonym_term, synonym_case_significant)
select concept_id, description_id as synonym_description_id, term as synonym_term,
case when case_significance_id = 900000000000017005 then 1 else 0 end as synonym_case_significant
from synonym_description_id
natural join description_load
where active = 1
and language_code = 'en'
and type_id = 900000000000013009; -- synonym

drop table if exists fully_specified_name;

create table fully_specified_name (
	concept_id bigint not null unique,
	fully_specified_name_description_id bigint not null primary key,
	fully_specified_name text not null,
	fully_specified_name_case_significant smallint not null
);

insert into fully_specified_name
select concept_id,
	description_id as fully_specified_name_description_id,
	term as fully_specified_name,
	case when case_significance_id = 900000000000017005 then 1 else 0 end as fully_specified_name_case_significant
from description_load
where active = 1
	and type_id = 900000000000003001; -- fully specified name
    
select * from fully_specified_name;

drop table if exists description_active;

create table description_active as
select concept_id,
	description_id,
	term,
	case_significance_id,
	case when type_id = 900000000000003001 then 1 else 0 end as is_fsn,
	case when type_id = 900000000000013009 and ifnull(acceptability_id, 0) = 900000000000548007 then 1 else 0 end as is_pt
from description_load
	natural left outer join (select description_id, acceptability_id from gb_en_lang_refset_active) a
where active = 1
	and language_code = 'en';
    
alter table description_active add primary key (description_id);
alter table description_active add foreign key (concept_id) references concept_load (concept_id);

drop table if exists concept;

create table concept (
	concept_id bigint not null primary key,
	concept_effective_time date not null,
	concept_active smallint not null,
	fully_specified_name_description_id bigint not null,
	fully_specified_name text not null,
	fully_specified_name_case_significant smallint not null,
	preferred_term_description_id bigint not null,
	preferred_term text not null,
	preferred_term_case_significant smallint not null,
	fully_defined smallint not null,
	ctv3_code varchar(5)
);

insert into concept (concept_id, concept_effective_time, concept_active,
fully_specified_name_description_id,
fully_specified_name, fully_specified_name_case_significant,
preferred_term_description_id, preferred_term, preferred_term_case_significant,
fully_defined, ctv3_code)
select concept_id, effective_time, active,
fully_specified_name_description_id, fully_specified_name, fully_specified_name_case_significant, 
preferred_term_description_id, preferred_term, preferred_term_case_significant,
case when definition_status_id = 900000000000073002 then 1 else 0 end as fully_defined, ctv3_code
from concept_load
natural join fully_specified_name
natural join preferred_term
natural left outer join ctv3_snomed_map
where active = 1;

alter table concept add unique (fully_specified_name_description_id);
alter table concept add unique (preferred_term_description_id);
alter table concept add unique (ctv3_code);

drop table if exists concept_active_or_inactive;

create table concept_active_or_inactive (
	concept_id bigint not null primary key,
	concept_effective_time date not null,
	concept_active smallint not null,
	fully_specified_name_description_id bigint not null,
	fully_specified_name text not null,
	fully_specified_name_case_significant smallint not null,
	preferred_term_description_id bigint not null,
	preferred_term text not null,
	preferred_term_case_significant smallint not null,
	fully_defined smallint not null
);

insert into concept_active_or_inactive (concept_id, concept_effective_time, concept_active,
fully_specified_name_description_id,
fully_specified_name, fully_specified_name_case_significant,
preferred_term_description_id, preferred_term, preferred_term_case_significant,
fully_defined)
select concept_id, effective_time, active,
fully_specified_name_description_id, fully_specified_name, fully_specified_name_case_significant, 
preferred_term_description_id, preferred_term, preferred_term_case_significant,
case when definition_status_id = 900000000000073002 then 1 else 0 end as fully_defined
from concept_load
natural join fully_specified_name
natural join preferred_term;

alter table concept_active_or_inactive add unique (fully_specified_name_description_id);
alter table concept_active_or_inactive add unique (preferred_term_description_id);

drop table if exists concept_synonym;

create table concept_synonym as select * from concept natural join synonym_term;

alter table concept_synonym add primary key (synonym_description_id);

/* Transitive closure */

drop table if exists active_core_relationship;

create table active_core_relationship as
select relationship_id,
	effective_time,
	source_id,
	destination_id,
	type_id /* is-a, assoc morph, finding site etc */,
	characteristic_type_id /* stated relationship, inferred relationship */,
	modifier_id /* constant = existential modifier */
from relationship_load
where module_id = 900000000000207008 -- core module
and active = 1;

alter table active_core_relationship add primary key (relationship_id);

drop table if exists is_a_relationship;

create table is_a_relationship as
select source_id as sub_concept_id, destination_id as super_concept_id
from active_core_relationship
where type_id = 116680003 /* is-a relationship */;

alter table is_a_relationship add primary key (sub_concept_id, super_concept_id);
alter table is_a_relationship add unique (super_concept_id, sub_concept_id);

-- Transitive closure - run the following statement a few times until now rows are inserted and then stop (six iterations in July 2018)

insert into is_a_relationship (sub_concept_id, super_concept_id)
select distinct iar.sub_concept_id, parent_iar.super_concept_id
from is_a_relationship as iar
inner join is_a_relationship as parent_iar
on iar.super_concept_id = parent_iar.sub_concept_id
left outer join is_a_relationship as t2
on iar.sub_concept_id = t2.sub_concept_id and parent_iar.super_concept_id = t2.super_concept_id
where t2.sub_concept_id is null;

/* Associated morphology and finding site */

drop table if exists assoc_morph_relationship;

create table assoc_morph_relationship as
select distinct source_id as concept_id,destination_id as assoc_morph_concept_id
from relationship where type_id = 116676008 /* associated morphology (attribute) */;

alter table assoc_morph_relationship add primary key (concept_id, assoc_morph_concept_id);

drop table if exists assoc_morph;

create table assoc_morph as
select concept_id as assoc_morph_concept_id,
fully_specified_name as assoc_morph_fully_specified_name
from concept;

alter table assoc_morph add primary key (assoc_morph_concept_id);

drop table if exists finding_site_relationship;

create table finding_site_relationship as
select distinct source_id as concept_id,destination_id as finding_site_concept_id
from relationship where type_id = 363698007 /* finding site (attribute) */;

alter table finding_site_relationship add primary key (concept_id, finding_site_concept_id);

drop table if exists finding_site;

create table finding_site as select concept_id as finding_site_concept_id,
fully_specified_name as finding_site_fully_specified_name
from concept;

alter table finding_site add primary key (finding_site_concept_id);

-- Load Read v2 tables

drop table if exists read_term;
drop table if exists read_tab;
 
create table read_tab (
	read_code char(5) not null primary key,
	preferred_term_30 varchar(30) not null,
	preferred_term_60 varchar(60),
	preferred_term_198 varchar(198),
	icd9_code varchar(20),
	icd9_code_def varchar(2),
	icd9_cm_code varchar(20),
	icd9_cm_code_def varchar(2),
	opcs_42_code varchar(20),
	opcs_code_def varchar(2),
	specialty_flag varchar(10),
	status_flag smallint, -- constant = 0
	language_code varchar(2) not null -- constant = 'EN'
);

load data local infile '/Users/alastair/downloads/nhs_readv2_21/V2/Unified/Corev2.all'
into table read_tab 
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n';

select * from read_tab where read_code = 'R102.';

drop table if exists read_term_load;

create table read_term_load (
	term_key varchar(10) not null,
	uniquifier varchar(2) not null,
    term_30 varchar(30),
    term_60 varchar(60),
    term_198 varchar(198),
	term_code char(2) not null,
    language_code varchar(2) not null,
    read_code char(5) not null,
	status_flag smallint not null -- always 0?
);

load data local infile '/Users/alastair/downloads/nhs_readv2_21/V2/Unified/Keyv2.all'
into table read_term_load 
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n';

alter table read_term_load add primary key (read_code, term_code, uniquifier);

select * from read_term_load where read_code = 'R102.';

create table read_term as select distinct read_code, term_code as read_term_code,
	case when ifnull(term_198, '') <> '' then term_198 when ifnull(term_60, '') <> '' then term_60 else ifnull(term_30, '') end as read_term
from read_term_load;

alter table read_term add primary key (read_code, read_term_code);
alter table read_term add foreign key (read_code) references read_tab (read_code);
alter table read_term add check (read_term <> '');

select * from read_term where read_code = 'R102.';

/* The end */

