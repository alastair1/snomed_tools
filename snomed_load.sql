/* Load SNOMED RF2 distribution files (current snapshot) into a MySQL database */

create database if not exists snomed default collate latin1_general_cs;

use snomed;

/* Mappings */

drop table if exists map_load;

create table map_load (
id char(36) not null primary key,  
effective_time date not null,
active smallint not null,
module_id bigint not null,     
refset_id bigint not null, 
referenced_component_id bigint not null,
map_target varchar(20) not null
);

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Refset/Map/der2_sRefset_SimpleMapSnapshot_INT_20140131.txt'
into table map_load
ignore 1 lines;

drop table if exists ctv3_map;

create table ctv3_map as
select referenced_component_id as concept_id, replace(map_target, char(13), '') as read_code/*,
hex(replace(map_target, char(13), '')) as read_code_hex*/
from map_load
where refset_id = 900000000000497000 /* ctv3 */
and active = 1;

alter table ctv3_map add primary key (concept_id);

select count(*) from map_load where refset_id = 900000000000498005 /* snomed rt */;
select count(*) from map_load where refset_id = 446608001 /* icd-0 */;

/* Concepts */

drop table if exists concept_load;

create table concept_load (
concept_id bigint not null primary key,
effective_time date not null,
active smallint not null,
module_id bigint not null,
definition_status_id bigint not null
);

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Terminology/sct2_Concept_Snapshot_INT_20140131.txt'
into table concept_load
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

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Terminology/sct2_Description_Snapshot-en_INT_20140131.txt'
into table description_load
ignore 1 lines;

drop table if exists text_definitions_load;

create table text_definitions_load (
textdefinitionid bigint not null primary key,
effective_time date not null,
active smallint not null,
module_id bigint not null,
concept_id bigint not null,
language_code char(2) not null,
type_id bigint not null,
term text not null,
case_significance_id bigint not null
);

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Terminology/sct2_TextDefinition_Snapshot-en_INT_20140131.txt'
into table text_definitions_load
ignore 1 lines;

drop table relationship_load;

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

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Terminology/sct2_Relationship_Snapshot_INT_20140131.txt'
into table relationship_load
ignore 1 lines;

drop table if exists relationship;

create table relationship as
select relationship_id,effective_time,source_id,destination_id,relationship_group,type_id,
case when characteristic_type_id = 900000000000011006 then 1 else 0 end as stated
from relationship_load
where active;

alter table relationship add primary key (relationship_id);

drop table if exists stated_relationship_load;

create table stated_relationship_load (
relationship_id bigint not null primary key,
effective_time date not null,
active smallint not null,
module_id bigint not null /* 2 values: "core module" versus "model component module" */,
source_id bigint not null,
destination_id bigint not null,
relationship_group smallint not null /* 0-7 */,
type_id bigint not null /* 58 values */,
characteristic_type_id bigint not null /* constant: "stated defining relationship" */,
modifier_id bigint not null /* constant */
);

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Terminology/sct2_StatedRelationship_Snapshot_INT_20140131.txt'
into table stated_relationship_load
ignore 1 lines;

drop table if exists stated_relationship;

create table stated_relationship as
select relationship_id,effective_time,source_id,destination_id,relationship_group,type_id
from stated_relationship_load
where active;

alter table stated_relationship add primary key (relationship_id);
alter table stated_relationship add unique (source_id,destination_id,relationship_group,type_id);

/* Preferred terms */

drop table if exists language_refset;

create table language_refset (
id char(36) not null primary key,  
effective_time date not null,
active smallint not null,
module_id bigint not null,     
refset_id bigint not null, 
referenced_component_id bigint not null,
acceptability_id bigint not null
);

load data 
infile 'SnomedCT_Release_INT_20140131/RF2Release/Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-en_INT_20140131.txt'
into table language_refset
ignore 1 lines;

drop table if exists preferred_term_description_id;

create table preferred_term_description_id as
select referenced_component_id as description_id
from language_refset
where active = 1
and refset_id = '900000000000508004' /* UK English */
and acceptability_id = '900000000000548007' /* preferred term */;

alter table preferred_term_description_id add primary key (description_id);

drop table if exists synonym_description_id;

create table synonym_description_id as
select referenced_component_id as description_id
from language_refset
where active = 1
and refset_id = '900000000000508004' /* UK English */
and acceptability_id = '900000000000549004' /* synonym */;

alter table synonym_description_id add primary key (description_id);

drop table if exists preferred_term;

create table preferred_term (
concept_id bigint not null primary key,
preferred_term_description_id bigint not null,
preferred_term text not null,
preferred_term_case_significant smallint not null
);

insert into preferred_term (concept_id, preferred_term_description_id, preferred_term, preferred_term_case_significant)
select concept_id,
description_id as preferred_term_description_id,
term as preferred_term,
case when case_significance_id = '900000000000017005' then 1 else 0 end as preferred_term_case_significant
from preferred_term_description_id
natural join description_load
where active = 1
and language_code = 'en'
and type_id = '900000000000013009' /* preferred term or synonym */;

drop table if exists synonym_term;

create table synonym_term (
concept_id bigint not null,
synonym_description_id bigint not null,
synonym_term text not null,
synonym_case_significant smallint not null,
primary key (synonym_description_id)
);

insert into synonym_term (concept_id, synonym_description_id, synonym_term, synonym_case_significant)
select concept_id, description_id as synonym_description_id, term as synonym_term,
case when case_significance_id = '900000000000017005' then 1 else 0 end as synonym_case_significant
from synonym_description_id
natural join description_load
where active = 1
and language_code = 'en'
and type_id = '900000000000013009' /* preferred term or synonym */;

drop table if exists fully_specified_name;

create table fully_specified_name (
concept_id bigint not null,
fully_specified_name_description_id bigint not null primary key,
fully_specified_name text not null,
fully_specified_name_case_significant smallint not null
);

insert into fully_specified_name
select concept_id,
description_id as fully_specified_name_description_id,
term as fully_specified_name,
case when case_significance_id = '900000000000017005' then 1 else 0 end as fully_specified_name_case_significant
from description_load
where active = 1
and type_id = '900000000000003001' /* fully specified name */;

drop table if exists concept;

create table concept (
concept_id bigint not null primary key,
fully_specified_name_description_id bigint not null,
fully_specified_name text not null,
fully_specified_name_case_significant smallint not null,
preferred_term_description_id bigint not null,
preferred_term text not null,
preferred_term_case_significant smallint not null,
fully_defined smallint not null,
synonym_count smallint not null,
read_code varchar(5)
);

insert into concept (concept_id, fully_specified_name_description_id,
fully_specified_name, fully_specified_name_case_significant,
preferred_term_description_id, preferred_term, preferred_term_case_significant,
fully_defined, read_code)
select concept_id,
fully_specified_name_description_id, fully_specified_name, fully_specified_name_case_significant, 
preferred_term_description_id, preferred_term, preferred_term_case_significant,
case when definition_status_id = '900000000000073002' then 1 else 0 end as fully_defined,read_code
from concept_load
natural join fully_specified_name
natural join preferred_term
natural left outer join ctv3_map
where active = 1;

alter table concept add unique (fully_specified_name_description_id);
alter table concept add unique (preferred_term_description_id);
alter table concept add unique (read_code);

drop table if exists concept_synonym;

create table concept_synonym as select * from concept natural join synonym_term;

alter table concept_synonym add primary key (synonym_description_id);

drop table if exists assoc_morph_relationship;

create table assoc_morph_relationship as
select distinct source_id as concept_id,destination_id as assoc_morph_concept_id
from relationship where type_id = 116676008 /* associated morphology (attribute) */;

alter table assoc_morph_relationship add primary key (concept_id, assoc_morph_concept_id);

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

drop table if exists assoc_morph;

create table assoc_morph as
select concept_id as assoc_morph_concept_id,
fully_specified_name as assoc_morph_fully_specified_name
from concept;

alter table assoc_morph add primary key (assoc_morph_concept_id);

/* The end */
