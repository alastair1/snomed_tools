/* nz_extension_feb2019.sql
 *
 * Create SNOMED CT NZ Extension RF2 distribution in a MySQL database and write output files
 *
 * Copy the previous release files and apply adds, updates and deletes for this release
 *
 * It's important that when a refset member is dropped and then re-added the same UUID should be used
 *
 * Preliminary steps:
 * 1. Copy previous release directory to a new one with the new name
 * 2. Make new directory and all sub directories rwxrwxrwx
 * 3. Hand-edit new elements into the concept, description and relationship files in the Full/Terminology directory
 * 4. Copy this script and find/replace the latest and previous release dates
 * 5. Extract member files from refset tool and overwrite previous versions in the load directory
 * 6. Do other preparatory stuff in nz_extension_prep script
 * 7. Load new versions of subsets and maps and generate deltas with respect to the previous release
 * ...
 *
 * FIX do I need to create refset metadata for language refsets?
 * FIX intensional refsets like microorganism refset
 *
 * FIX rename the 'member' tables
 * FIX is simple refset upsert logic correct?
 *
 */

use snomed_jan2019; -- Extension to International Release Jan 2019

set @@global.local_infile="ON";
show variables like "local_infile";

-- Functions

drop function checksum_verhoeff;

delimiter $$

CREATE FUNCTION checksum_verhoeff(pnumber bigint, paction tinyint unsigned) -- 0 check, 1 generate
	RETURNS tinyint(4)
    deterministic
BEGIN
	DECLARE c tinyint unsigned;
	DECLARE len int;
	DECLARE m tinyint unsigned;
	DECLARE n varchar(255);
	DECLARE i smallint;
	DECLARE t int;
	DECLARE d char(100);
	DECLARE p char(80);
	DECLARE inv char(10);
	SET d = '0123456789123406789523401789563401289567401239567859876043216598710432765982104387659321049876543210';
	SET p = '01234567891576283094580379614289160435279453126870428657390127938064157046913258';
	SET inv = '0432156789';
	SET c = 0;
	SET n = Reverse(pnumber);
	SET len = Char_length(rtrim(n));
	set i = 0;
	WHILE i < len
	DO
		IF paction = 1 THEN	
			SET m = substring(p,(((i+1)%8)*10)+ substring(n,i+1,1) +1,1); 
		ELSE		
			SET m = substring(p,((i%8)*10)+ substring(n,i+1,1)+1,1);
		END IF;
		SET c = substring(d,(c*10+m+1),1);
		SET i = i + 1;
	END WHILE;
	IF paction = 1 THEN
		SET c = substring(inv, c+1, 1);
	END IF;
	return (c);
END;
$$

delimiter ;

-- Drop tables

drop table if exists nz_simple_refset_full;
drop table if exists nz_simple_refset;
drop table if exists nz_simple_refset_snapshot;
drop table if exists nz_simple_refset_snapshot_final;
drop table if exists nz_simple_refset_active;
drop table if exists nz_simple_refset_active_;
drop table if exists nz_simple_refset_add;
drop table if exists nz_simple_refset_upd;
drop table if exists nz_simple_refset_del;

drop table if exists nz_module_dep;
drop table if exists nz_module_dep_snapshot;
drop table if exists nz_description_snapshot;
drop table if exists nz_relationship_snapshot;
drop table if exists nz_stated_relationship;
drop table if exists nz_stated_relationship_snapshot;

drop table if exists nz_simple_map_active;
drop table if exists nz_simple_map_full;
drop table if exists nz_simple_map_snapshot;
drop table if exists nz_simple_map;
drop table if exists nz_simple_map_load_snapshot;

drop table if exists acc_s2r_map_active_load;
drop table if exists msd_s2r_map_active_load;
drop table if exists nz_r2s_map_active_load;
drop table if exists nz_s2r_map_active_load;

drop table if exists nzmt_sct_map_load;
drop table if exists nzmt_sct_map;

drop table if exists nz_r2s_map;
drop table if exists nz_s2r_map;
drop table if exists nz_s2r_map_snapshot;

drop table if exists nz_en_lang_refset;
drop table if exists nz_en_lang_refset_snapshot;
drop table if exists nz_en_pft_lang_refset_load;
drop table if exists nz_en_lang_refset_snapshot_final;
drop table if exists nz_mi_lang_refset;
drop table if exists nz_mi_lang_refset_snapshot;
drop table if exists nz_en_lrs_member_active;

drop table if exists nz_refset_attribute;
drop table if exists nz_description;
drop table if exists nz_relationship;
drop table if exists nz_concept_snapshot;
drop table if exists nz_concept;

drop table if exists all_concept;
drop table if exists all_description;

drop table if exists nz_module_release;
drop table if exists nz_module, nz_release;

/* Modules and releases */

create table nz_module (module_id bigint not null primary key);

insert into nz_module values (21000210109);

create table nz_release (effective_date date not null primary key);

insert into nz_release (effective_date)
	select date '2016-10-01'
    union select date '2017-07-01'
    union select date '2017-08-01'
    union select date '2017-11-01'
    union select date '2018-03-01'
    union select date '2018-05-01'
    union select date '2018-08-01'
    union select date '2018-11-01'
    union select date '2019-02-01'; -- this release Feb 2019
    
create table nz_module_release (module_id bigint not null, effective_date date not null, primary key (module_id, effective_date));

insert into nz_module_release select * from nz_module natural join nz_release;

alter table nz_module_release add foreign key (module_id) references nz_module (module_id);
alter table nz_module_release add foreign key (effective_date) references nz_release (effective_date);

create table nz_module_dep (
	dep_id char(36) not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
    refset_id bigint not null,
	referenced_component_id bigint not null,
    source_effective_date date not null,
    target_effective_date date not null,
	primary key (dep_id, effective_date)
);

load data local -- the previous release that we are adding to (16 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Metadata/der2_ssRefset_ModuleDependencyFull_NZ1000210_20181101.txt'
into table nz_module_dep
lines terminated by '\r\n'
ignore 1 lines;

-- Add two dependencies per NZ release: on both the SNOMED CT core module and the SNOMED CT model component module in the latest international release

insert into nz_module_dep (dep_id, effective_date, active, module_id, refset_id, referenced_component_id, source_effective_date, target_effective_date)
select dep_id, -- {dep_id} <-> {module_id, referenced_component_id} (2 rows)
	date '2019-02-01' as effective_date, -- this NZ Extension release
    1 as active,
    module_id,
    refset_id,
    referenced_component_id, -- SNOMED CT core module, SNOMED CT model component module
    date '2019-02-01' as source_effective_date, -- this release (NZ Extension)
    date '2019-01-31' as target_effective_date -- latest International Release
from nz_module_dep
where effective_date = date '2018-11-01'; -- previous release

alter table nz_module_dep add foreign key (module_id, source_effective_date) references nz_module_release (module_id, effective_date);

select * from nz_module_dep -- 2 rows
where effective_date = date '2019-02-01' -- this release
order by effective_date, referenced_component_id;

create table nz_module_dep_snapshot as select dep_id, max(effective_date) as effective_date from nz_module_dep group by dep_id;

alter table nz_module_dep_snapshot add primary key (dep_id);

/* Concepts
 *
 * First manually add new concepts into 'full' distribution file
 *
 * The new concepts for this release are:
 * NZ English patient friendly terms language refset
 * Endocrinology refset
 *
 * This release also includes the new synonyms needed for the patient friendly terms included in the NZ English PFT language refset
 *
 */

create table nz_concept (
	concept_id bigint not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	definition_status_id bigint not null,
	primary key (concept_id, effective_date)
);

load data local -- this release with new concepts already added by hand (34 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Terminology/sct2_Concept_Full_NZ1000210_20190201.txt'
into table nz_concept
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_concept add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

select distinct substring(concept_id, length(concept_id) - 9, 7) as namespace_id, -- 1000210, 10, 0
	substring(concept_id, length(concept_id) - 2, 2) as partition_id, -- 10
    checksum_verhoeff(concept_id, 0) as check_digit -- 0
from nz_concept;

select count(*) from nz_concept where effective_date = date '2019-02-01'; -- this release (6 rows)

create table nz_concept_snapshot as select concept_id, max(effective_date) as effective_date from nz_concept group by concept_id;

alter table nz_concept_snapshot add primary key (concept_id);

create table all_concept as select concept_id, 1 as active from nz_concept union select concept_id, active from concept_load;

alter table all_concept add primary key (concept_id);

/* Descriptions
 *
 * First manually add new descriptions into 'full' distribution file
 *
 */

create table nz_description (
	description_id bigint not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	concept_id bigint not null,
	language_code char(2) not null,
	type_id bigint not null, -- FSN, PT
	term text not null,
	case_significance_id bigint not null, -- (fully) case sensitive for FSN, case insensitive for PT
    primary key (description_id, effective_date)
);

load data local -- this release with new descriptions already added by hand (186 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Terminology/sct2_Description_Full-en-NZ_NZ1000210_20190201.txt'
into table nz_description
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_description add foreign key (concept_id) references all_concept (concept_id);
alter table nz_description add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

select distinct substring(description_id, length(description_id) - 9, 7) as namespace_id, -- 1000210, 11, 0
	substring(description_id, length(description_id) - 2, 2) as partition_id, -- 11
    checksum_verhoeff(description_id, 0) as check_digit -- 0
from nz_description;

select count(*) from nz_description where effective_date = date '2019-02-01'; -- this release (103 rows)

select distinct type_id, case_significance_id, language_code from nz_description where type_id = 900000000000003001; -- FSN
select distinct type_id, case_significance_id, language_code from nz_description where type_id <> 900000000000003001; -- PT

create table nz_description_snapshot as select description_id, max(effective_date) as effective_date from nz_description group by description_id;

alter table nz_description_snapshot add primary key (description_id);

create table all_description as select description_id, 1 as active from nz_description union select description_id, active from description_load;

alter table all_description add primary key (description_id);

/* Relationships
 *
 * First manually add new relationships into 'full' distribution file
 *
 */

create table nz_relationship (
	relationship_id bigint not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	source_id bigint not null,
	destination_id bigint not null,
	relationship_group smallint not null,
	type_id bigint not null,
	characteristic_type_id bigint not null /* inferred relationship */,
	modifier_id bigint not null,
    primary key (relationship_id, effective_date)
);

load data local -- this release with new relationships already added by hand (42 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Terminology/sct2_Relationship_Full_NZ1000210_20190201.txt'
into table nz_relationship
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_relationship add foreign key (source_id) references nz_concept_snapshot (concept_id);
alter table nz_relationship add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

select distinct substring(relationship_id, length(relationship_id) - 9, 7) as namespace_id, -- 1000210, 12, 9...11006, 0
	substring(relationship_id, length(relationship_id) - 2, 2) as partition_id, -- 12
    characteristic_type_id, -- inferred relationship
    checksum_verhoeff(relationship_id, 0) as check_digit -- 0
from nz_relationship;

select count(*) from nz_relationship where effective_date = date '2019-02-01'; -- this release (6 rows)

create table nz_relationship_snapshot as select relationship_id, max(effective_date) as effective_date from nz_relationship group by relationship_id;

alter table nz_relationship_snapshot add primary key (relationship_id);

/* Stated relationships
 *
 * First manually add new stated relationships into 'full' distribution file 
 *
 * July 2019 international release will not have stated relationships table 
 *
 */

create table nz_stated_relationship (
	relationship_id bigint not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	source_id bigint not null,
	destination_id bigint not null,
	relationship_group smallint not null,
	type_id bigint not null,
	characteristic_type_id bigint not null /* stated relationship */,
	modifier_id bigint not null,
    primary key (relationship_id, effective_date)
);

load data local -- this release with new relationships already added by hand (42 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Terminology/sct2_StatedRelationship_Full_NZ1000210_20190201.txt'
into table nz_stated_relationship
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_stated_relationship add foreign key (source_id) references nz_concept_snapshot (concept_id);
alter table nz_stated_relationship add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

select distinct substring(relationship_id, length(relationship_id) - 9, 7) as namespace_id, -- 1000210, 12, 9...10007, 0
	substring(relationship_id, length(relationship_id) - 2, 2) as partition_id, -- 12
    characteristic_type_id, -- stated relationship
    checksum_verhoeff(relationship_id, 0) as check_digit -- 0
from nz_stated_relationship;

select count(*) from nz_stated_relationship where effective_date = date '2019-02-01'; -- this release (6 rows)

create table nz_stated_relationship_snapshot as select relationship_id, max(effective_date) as effective_date from nz_stated_relationship group by relationship_id;

alter table nz_stated_relationship_snapshot add primary key (relationship_id);

/* Simple maps
 *
 * Map data comes from different sources - needs to be handled carefully
 *
 * R2S map is a localised version of the UK original
 * S2R map is from the UK primary care refset R-S equivalence table, overlaid with local changes
 * ACC and MSD S2R maps are subsets of the full S2R map
 * Source data for NZMT medicinal product map is maintained by NZULM team and repackaged here
 *
 */

create table nz_simple_map as -- 5 rows
select source_id as refset_id
from nz_relationship
where destination_id = 900000000000496009;

alter table nz_simple_map add primary key (refset_id);
alter table nz_simple_map add foreign key (refset_id) references nz_concept (concept_id);

create table nz_simple_map_full ( -- the members
	member_id char(36) not null, -- FDs {member_id} <-> {refset_id, referenced_component_id, map_target}
	effective_date date not null,
	active smallint not null,
	module_id bigint not null /* 21000210109 */,     
	refset_id bigint not null /* R2S, S2R, NZMT-SCT, ACC S2R, MSD S2R */, 
	referenced_component_id bigint not null /* concept_id */,
	map_target varchar(20) not null,
	primary key (member_id, effective_date)
);


load data local -- the previous release (240346 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Map/der2_sRefset_SimpleMapFull_NZ1000210_20181101.txt'
into table nz_simple_map_full
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_simple_map_full add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);
alter table nz_simple_map_full add foreign key (refset_id) references nz_simple_map (refset_id);
alter table nz_simple_map_full add foreign key (referenced_component_id) references concept_load (concept_id);

select refset_id, count(*) as n from nz_simple_map_full group by refset_id order by refset_id; -- R2S 92115 rows, S2R 123235, NZMT 2425, ACC 10142, MSD 12429

select refset_id, referenced_component_id, map_target, count(distinct member_id) as n -- check {refset_id, referenced_component_id, map_target} -> {member_id}
from nz_simple_map_full
group by refset_id, referenced_component_id, map_target
having n > 1;

select member_id, count(*) as n -- check {member_id} -> {refset_id, referenced_component_id, map_target}
from (select distinct member_id, refset_id, referenced_component_id, map_target from nz_simple_map_full) a
group by member_id
having n > 1;

create table nz_simple_map_load_snapshot as -- 224549 rows
select member_id,
    effective_date,
    active,
    refset_id,
	concept_id,
	map_target
from (
	select member_id,
		refset_id,
		referenced_component_id as concept_id,
		map_target,
		max(effective_date) as effective_date
	from nz_simple_map_full
	group by member_id,
		refset_id,
		referenced_component_id,
		map_target
	) a
    natural join (
		select member_id,
			effective_date,
            active
		from nz_simple_map_full
	) b;

alter table nz_simple_map_load_snapshot add primary key (member_id);
alter table nz_simple_map_load_snapshot add unique (refset_id, concept_id, map_target);
alter table nz_simple_map_load_snapshot add foreign key (concept_id) references concept_load (concept_id);

create table nz_s2r_map_snapshot as -- 112366 rows 
select member_id,
	concept_id,
	substring(map_target, 1, 5) as read_code,
	substring(map_target, 6, 2) as read_term_code,
	effective_date,
    active
from nz_simple_map_load_snapshot
where refset_id = 41000210103; -- S2R map

alter table nz_s2r_map_snapshot add primary key (member_id);
alter table nz_s2r_map_snapshot add unique (concept_id, read_code, read_term_code);
alter table nz_s2r_map_snapshot add foreign key (concept_id) references concept_load (concept_id);

create table nz_simple_map_active ( -- members
	refset_id bigint not null, 
	concept_id bigint not null,
    map_target varchar(20) not null,
    primary key (refset_id, concept_id, map_target),
    foreign key (concept_id) references concept (concept_id)
);

-- Load prepared new version of R2S map

create table nz_r2s_map_active_load ( 
	concept_id bigint not null,
    read_code char(5) not null,
    read_term_code char(2) not null,
    primary key (read_code, read_term_code)
);

load data local -- 88273 rows (from PFTs and also the latest R2S map from NHS Digital)
infile '/users/alastair/documents/snomed/nz_r2s_map_feb2019.txt' 
into table nz_r2s_map_active_load
lines terminated by '\r\n'
ignore 1 lines;

insert into nz_simple_map_active (refset_id, concept_id, map_target) -- 87343 rows
select 31000210106 as refset_id, -- R2S map
	concept_id,
	concat(read_code, read_term_code) as map_target
from nz_r2s_map_active_load
	natural join (select concept_id from concept) a;
    
-- Load S2R map
--
-- Use nz_extension_prep_feb2019.sql script to create the s2r map input
--
-- MSD map is a superset of the ACC map and a subset of the overall NZ S2R map (which is based on the NHS Digital primary care refset equivalence table
-- The input files must be prepared this way

create table nz_s2r_map_active_load ( -- prepared active NZ mappings
	concept_id bigint not null primary key,
	read_code char(5) not null,
    read_term_code char(2) not null
);

load data local -- 55080 rows
infile '/users/alastair/documents/snomed/nz_s2r_map_feb2019.txt' 
into table nz_s2r_map_active_load
lines terminated by '\r\n'
ignore 1 lines;

select * from nz_s2r_map_active_load where checksum_verhoeff(concept_id, 0) <> 0; -- should be empty

alter table nz_s2r_map_active_load add foreign key (concept_id) references concept_load (concept_id);

insert into nz_simple_map_active (refset_id, concept_id, map_target) -- 55080 rows
select 41000210103 as refset_id, -- S2R map
	concept_id,
	concat(read_code, read_term_code) as map_target
from nz_s2r_map_active_load
	natural join (select concept_id from concept) a;

-- Load MSD S2R map (subset of the full S2R map and superset of the ACC S2R map)

create table msd_s2r_map_active_load ( -- prepared active MSD mappings
	concept_id bigint not null primary key,
	read_code char(5) not null,
    read_term_code char(2) not null
);

load data local -- 55080 rows (using whole NZ S2R map)
infile '/users/alastair/documents/snomed/msd_s2r_map_feb2019.txt' 
into table msd_s2r_map_active_load
lines terminated by '\r\n'
ignore 1 lines;

select * from msd_s2r_map_active_load where checksum_verhoeff(concept_id, 0) <> 0; -- should be empty

alter table msd_s2r_map_active_load add foreign key (concept_id) references concept_load (concept_id);

insert into nz_simple_map_active (refset_id, concept_id, map_target) -- 55080 rows created
select 411000210104 as refset_id, -- MSD S2R map 
	concept_id,
    concat(read_code, read_term_code) as map_target
from msd_s2r_map_active_load
	natural join (select concept_id from concept) a;
    
-- Load ACC S2R map (subset of the MSD S2R map)

create table acc_s2r_map_active_load ( -- currently active ACC mappings
	concept_id bigint not null primary key,
	read_code char(5) not null,
    read_term_code char(2) not null
);

load data local -- 9890 rows
infile '/users/alastair/documents/snomed/acc_s2r_map_feb2019.txt'
into table acc_s2r_map_active_load
lines terminated by '\r\n'
ignore 1 lines;

select * from acc_s2r_map_active_load where checksum_verhoeff(concept_id, 0) <> 0; -- should be empty

alter table acc_s2r_map_active_load add foreign key (concept_id) references concept_load (concept_id);

select * from acc_s2r_map_active_load -- empty if MSD a superset of ACC mappings
natural left outer join msd_s2r_map_active_load a
where a.concept_id is null; 

insert into nz_simple_map_active (refset_id, concept_id, map_target) -- 9890 rows created
select 401000210101 as refset_id, -- ACC S2R map
	concept_id,
    concat(read_code, read_term_code) as map_target
from acc_s2r_map_active_load
	natural join (select concept_id from concept) a;

-- Load NZMT medicinal product to SNOMED map
-- Example: NZMT 'metformin' MP is mapped to SCT 'Product containing metformin (medicinal product)'

create table nzmt_sct_map_load (
	mp_id bigint not null,
    mp_term varchar(500) not null,
    effective_date date not null,
    description_id bigint not null,
    concept_id bigint not null,
    fully_specified_name varchar(150) not null
);

load data local -- 2081 rows
infile '/users/alastair/downloads/nzulm-2019-01/nzmt_to_snomed_ct_international_edition_map_dump.csv'
into table nzmt_sct_map_load
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\r\n'
ignore 1 lines;

alter table nzmt_sct_map_load add primary key (mp_id); -- {mp_id} -> {concept_id} -> {description_id}

select max(length(mp_term)), max(length(fully_specified_name)) from nzmt_sct_map_load; -- 499 chars, 142 chars

select count(distinct concept_id) from nzmt_sct_map_load; -- 1595

select mp_id, mp_term, concept_id, fully_specified_name -- 56 inactive concepts
from nzmt_sct_map_load
	natural left outer join (select concept_id, preferred_term from concept) a 
where a.concept_id is null;

select * from nzmt_sct_map_load -- check semantic tag (should be empty result set)
	natural left outer join (select concept_id, preferred_term, fully_specified_name from concept) a
where fully_specified_name not like '% (product)'
	and fully_specified_name not like '% (medicinal product)';
   
insert into nz_simple_map_active (refset_id, concept_id, map_target) -- 2025 rows
select 311000210107 as refset_id,
	concept_id,
	mp_id as map_target
from nzmt_sct_map_load
	natural join (select concept_id from concept) a;

-- Generate simple map deltas

insert into nz_simple_map_full (member_id, effective_date, active, module_id, refset_id, referenced_component_id, map_target) -- 56219 rows inactivated
select member_id,
	date '2019-02-01' as effective_date, -- this release
    0 as active,
    21000210109 as module_id, -- NZ module
    refset_id,
    concept_id,
    map_target
from nz_simple_map_load_snapshot
	natural left outer join nz_simple_map_active a
where active = 1
	and a.concept_id is null;

insert into nz_simple_map_full (member_id, effective_date, active, module_id, refset_id, referenced_component_id, map_target) -- 56785 new rows
select uuid() as member_id,
	date '2019-02-01' as effective_date, -- this release
    1 as active,
    21000210109 as module_id, -- NZ module
    refset_id,
    concept_id,
    map_target
from nz_simple_map_active
	natural left outer join nz_simple_map_load_snapshot
where member_id is null;

insert into nz_simple_map_full (member_id, effective_date, active, module_id, refset_id, referenced_component_id, map_target) -- 20 rows reactivated
select member_id, 
	date '2019-02-01' as effective_date, -- this release
    1 as active,
    21000210109 as module_id, -- NZ module
    refset_id,
    concept_id,
    map_target
from nz_simple_map_active
	natural join nz_simple_map_load_snapshot
where active = 0;

create table nz_simple_map_snapshot as -- 281334 rows
select member_id,
    effective_date,
    active,
    refset_id,
	concept_id,
	map_target
from (
	select member_id,
		refset_id,
		referenced_component_id as concept_id,
		map_target,
		max(effective_date) as effective_date
	from nz_simple_map_full
	group by member_id,
		refset_id,
		referenced_component_id,
		map_target
	) a
    natural join (
		select member_id,
			effective_date,
            active
		from nz_simple_map_full
	) b;

alter table nz_simple_map_snapshot add primary key (member_id);
alter table nz_simple_map_snapshot add unique (refset_id, concept_id, map_target);
alter table nz_simple_map_snapshot add foreign key (concept_id) references concept_load (concept_id);

-- Simple map integrity checks

create table nz_r2s_map as -- 87343 rows
select concept_id, substring(map_target, 1, 5) as read_code, substring(map_target, 6, 2) as read_term_code
from nz_simple_map_snapshot
where refset_id = 31000210106 -- R2S map
	and active = 1;

alter table nz_r2s_map add primary key (read_code, read_term_code);
alter table nz_r2s_map add foreign key (concept_id) references concept (concept_id);

create table nz_s2r_map as -- 55080 rows
select concept_id, substring(map_target, 1, 5) as read_code, substring(map_target, 6, 2) as read_term_code
from nz_simple_map_snapshot
where refset_id = 41000210103 -- S2R map
	and active = 1;

alter table nz_s2r_map add primary key (concept_id);
alter table nz_s2r_map add foreign key (concept_id) references concept (concept_id);

create table nzmt_sct_map as -- 2025 rows
select concept_id, map_target as mp_id
from nz_simple_map_snapshot
where refset_id = 311000210107 -- NZMT-SCT map
	and active = 1;

alter table nzmt_sct_map add primary key (mp_id);
alter table nzmt_sct_map add foreign key (concept_id) references concept (concept_id);

-- Simple map counts

select refset_id, count(*) as n -- the number active per map (R2S 87343 rows, S2R 55080, NZMT-SCT 2025, ACC S2R 9890, MSD S2R 55080)
from nz_simple_map_full
	natural join nz_simple_map_snapshot
where active = 1
group by refset_id
order by refset_id;

select refset_id, active, count(*) as n -- activated/inactivated per map (R2S -956 +5612, S2R -54762 +8273, NZMT-SCT -54 +0, ACC S2R -216 +0, MSD S2R -231 +42920)
from nz_simple_map_full
where effective_date = date '2019-02-01' -- this release
group by refset_id, active
order by refset_id, active;

-- Simple map delta examples

select * from nz_simple_map_full
where refset_id = 41000210103 and referenced_component_id = 1376001 order by effective_date, active; -- sample changed mappings

select * from nz_simple_map_full
where refset_id = 41000210103 and referenced_component_id = 368009 order by effective_date, active; -- sample changed mappings

select * from nz_simple_map_full -- an example of a concept referenced in several maps
where referenced_component_id = 282776008
order by refset_id, effective_date, active; 

select * from nz_simple_map_full -- example of map properly changed to different read code in S2R map
where refset_id = 41000210103 and referenced_component_id = 122003
order by effective_date, active;

select * from nz_simple_map_full -- showing inactivated concept has been dealt with in two maps
where referenced_component_id = 105441003 order by refset_id, effective_date;

select * from nz_simple_map_full -- concept now mapped to something else 
where refset_id = 41000210103
	and referenced_component_id = 282776008
order by effective_date, active; 

select * from nz_simple_map_full -- concept now mapped to something else (change made Aug 2018)
where refset_id = 41000210103
	and referenced_component_id = 1376001
order by effective_date, active; 

select * from nz_simple_map_full -- example of a map reverting to a former choice of READ code (the ACC choice now replacing the UK choice)
where refset_id = 41000210103
	and referenced_component_id = 16607004 
order by effective_date, active;

/* English language refsets including patient friendly terms
 *
 * Add new descriptions to NZ English LRS and reconcile with GB English LRS
 *
 * In the language refset tables {member_id} <-> {refset_id, description_id}
 */

create table nz_en_lang_refset (
	member_id char(36) not null,  
	effective_date date not null,
	active smallint not null,
	module_id bigint not null, -- 21000210109
	refset_id bigint not null, -- NZ English LRS, NZ English PFT LRS
	description_id bigint not null,
	acceptability_id bigint not null,
    primary key (member_id, effective_date)
);

load data local -- load previous release (1246384 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Language/der2_cRefset_LanguageFull-en-NZ_NZ1000210_20181101.txt'
into table nz_en_lang_refset
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_en_lang_refset add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

select refset_id, count(*) as n from nz_en_lang_refset group by refset_id order by refset_id; -- NZ English LRS 1246326 NZ English PFT LRS 58

create table nz_en_lang_refset_snapshot as -- 1192264 rows
select member_id,
    effective_date,
    active,
    refset_id,
	description_id,
	acceptability_id
from (
	select member_id,
		refset_id,
		description_id,
		max(effective_date) as effective_date
	from nz_en_lang_refset
	group by member_id,
		refset_id,
		description_id
	) a
    natural join (
		select member_id,
			effective_date,
            acceptability_id,
            active
		from nz_en_lang_refset
	) b;
    
alter table nz_en_lang_refset_snapshot add primary key (member_id);
alter table nz_en_lang_refset_snapshot add unique (refset_id, description_id);
alter table nz_en_lang_refset_snapshot add foreign key (description_id) references all_description (description_id);

select count(*) from nz_en_lang_refset_snapshot a
where not exists (select 1 from all_description b where b.description_id = a.description_id); -- 4176 rows (text definitions I want to get rid of)

/* eg 2884452019, 2884453012, 2884454018 text definitions

select * from nz_en_lang_refset_snapshot where description_id = 2884452019;
select * from gb_en_lang_refset_active where description_id = 2884452019;
select * from description_load where description_id = 2884452019;
*/

-- Load active PFT LRS 

create table nz_en_pft_lang_refset_load (
	read_code char(5) not null,
	read_term_code char(2) not null,
	read_term varchar(100) not null,
	pft varchar(100) not null,
    concept_id bigint not null,
    description_id bigint not null,
    primary key (read_code, read_term_code)
);

load data local infile '/users/alastair/documents/snomed/nz_en_pft_lang_refset_feb2019.txt' -- 157 rows
into table nz_en_pft_lang_refset_load
lines terminated by '\r\n'
ignore 1 lines
(read_code, read_term_code, read_term, pft, concept_id, description_id);

alter table nz_en_pft_lang_refset_load add foreign key (concept_id) references concept (concept_id);
alter table nz_en_pft_lang_refset_load add foreign key (description_id) references all_description (description_id); -- text definitions prevent this

-- Create table of all active LRS members

create table nz_en_lrs_member_active (
    refset_id bigint not null,
    description_id bigint not null,
    acceptability_id bigint not null,
    primary key (refset_id, description_id)
);

insert into nz_en_lrs_member_active (refset_id, description_id, acceptability_id) -- 1154981 rows
select 271000210107 as refset_id, -- NZ English LRS
	description_id,
    acceptability_id
from gb_en_lang_refset_active;

insert into nz_en_lrs_member_active (refset_id, description_id, acceptability_id) -- 157 rows
select 281000210109 as refset_id, -- NZ English PFT LRS
	description_id,
    900000000000548007 as acceptability_id -- PT
from nz_en_pft_lang_refset_load;

insert into nz_en_lrs_member_active (refset_id, description_id, acceptability_id) -- 72 rows
select 271000210107 as refset_id, -- NZ English LRS
	description_id,
    900000000000548007 as acceptability_id -- PT
from nz_description
where description_id not between 911000210112 and 2051000210118; -- PFT range

insert into nz_en_lang_refset (member_id, effective_date, active, module_id, refset_id, description_id, acceptability_id) -- 44221 rows (new members)
select uuid() as member_id,
	date '2019-02-01' as effective_date, -- this release 
	1 as active,
	21000210109 as module_id, -- NZ module
	refset_id,
	description_id,
	acceptability_id
from nz_en_lrs_member_active
	natural left outer join (select member_id, refset_id, description_id from nz_en_lang_refset_snapshot) a
where member_id is null;

insert into nz_en_lang_refset (member_id, effective_date, active, module_id, refset_id, description_id, acceptability_id) -- 29232 rows (inactivated)
select member_id,  
	date '2019-02-01' as effective_date, -- this release
	0 as active,
	21000210109 as module_id,
	refset_id,
	description_id,
	acceptability_id
from nz_en_lang_refset_snapshot
	natural left outer join (select refset_id, description_id from nz_en_lrs_member_active) a
where active = 1
	and a.description_id is null;

insert into nz_en_lang_refset (member_id, effective_date, active, module_id, refset_id, description_id, acceptability_id) -- 2207 rows (reactivated or updated)
select member_id,
	date '2019-02-01' as effective_date, -- this release
	1 as active,
    21000210109 as module_id, -- NZ module
    refset_id,
    description_id,
    new_acceptability_id
from nz_en_lang_refset_snapshot
	natural join (select refset_id, description_id, acceptability_id as new_acceptability_id from nz_en_lrs_member_active) a
where active = 0 or acceptability_id <> new_acceptability_id;

-- LRS snapshot

create table nz_en_lang_refset_snapshot_final as -- 1236485 rows
select member_id,
    effective_date,
    active,
    refset_id,
	description_id,
	acceptability_id
from (
	select member_id,
		refset_id,
		description_id,
		max(effective_date) as effective_date
	from nz_en_lang_refset
	group by member_id,
		refset_id,
		description_id
	) a
    natural join (
		select member_id,
			effective_date,
            acceptability_id,
            active
		from nz_en_lang_refset
	) b;
    
alter table nz_en_lang_refset_snapshot_final add primary key (member_id);
alter table nz_en_lang_refset_snapshot_final add unique (refset_id, description_id);
alter table nz_en_lang_refset_snapshot_final add foreign key (description_id) references all_description (description_id); -- text definitions prevent this

select refset_id, count(*) as n -- NZ en LRS 1155053, NZ en PFT LRS 157
from nz_en_lang_refset_snapshot_final
where active = 1
group by refset_id
order by refset_id; 

select refset_id, active, count(*) as n -- LRS members activated/inactivated this release (NZ en -29228 +46325, NZ en PFT -4 +103)
from nz_en_lang_refset
where effective_date = date '2019-02-01' -- this release
group by refset_id, active
order by refset_id, active;

select member_id, refset_id, description_id, active -- check that all descriptions that should be are active (should return empty set)
from (select description_id from nz_description_snapshot) a
	natural left outer join (select member_id, refset_id, description_id, effective_date, active from nz_en_lang_refset_snapshot_final) b
where active <> 1
order by effective_date, description_id;

-- Check exactly one PT per concept per LRS

select refset_id, count(*), count(distinct description_id) -- correct
from nz_en_lang_refset_snapshot_final
where active = 1
	and acceptability_id = 900000000000548007 -- PT
group by refset_id
order by refset_id;

-- Check that NZ English LRS matches GB English LRS

select count(*) from gb_en_lang_refset_active; -- 1154981 X

select count(*) from nz_description a
where not exists (select 1 from nz_en_lang_refset_snapshot_final where refset_id = 281000210109 /* NZ en PFT LRS */ and description_id = a.description_id); -- 72 Y

select count(*) -- 1155053 rows X + Y (correct)
from nz_en_lang_refset_snapshot_final
natural join nz_en_lang_refset
where refset_id = 271000210107 -- NZ en LRS
	and active = 1; 

select count(*) as n -- check all active GB descriptions are present in NZ English LRS - 1154981 should be X (correct)
from (select description_id from gb_en_lang_refset_active) a
	natural join (select description_id from nz_en_lang_refset_snapshot_final
		where refset_id = 271000210107 /* NZ en LRS */ and active = 1) b;

/* Maori language refset */

create table nz_mi_lang_refset (
	member_id char(36) not null,  
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	refset_id bigint not null, 
	referenced_component_id bigint not null,
	acceptability_id bigint not null,
    primary key (member_id, effective_date)
);

load data local -- load previous release (0 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Language/der2_cRefset_LanguageFull-mi_NZ1000210_20181101.txt'
into table nz_mi_lang_refset
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_mi_lang_refset add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);

create table nz_mi_lang_refset_snapshot as
select member_id, max(effective_date) as effective_date
from nz_mi_lang_refset
group by member_id;

alter table nz_mi_lang_refset_snapshot add primary key (member_id);

/* Content refsets */

create table nz_simple_refset as -- 21 rows
select source_id as refset_id
from nz_relationship
where destination_id = 446609009;

alter table nz_simple_refset add primary key (refset_id);
alter table nz_simple_refset add foreign key (refset_id) references nz_concept (concept_id);

/* Refset attributes */

create table nz_refset_attribute (
	attribute_id char(36) not null,
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	refset_id bigint not null,
	referenced_component_id bigint not null,
	attribute_description bigint not null,
	attribute_type bigint not null,
	attribute_order smallint not null,
    primary key (attribute_id, effective_date)
);

load data local -- load previous release (25 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Metadata/der2_cciRefset_RefsetDescriptorFull_NZ1000210_20181101.txt'
into table nz_refset_attribute
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_refset_attribute add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);
alter table nz_refset_attribute add foreign key (referenced_component_id) references nz_concept_snapshot (concept_id);

insert into nz_refset_attribute ( -- attributes for new simple refsets (6 rows)
	attribute_id,
	effective_date,
    active,
    module_id,
    refset_id,
    referenced_component_id,
    attribute_description,
    attribute_type,
    attribute_order
)
select uuid() as member_id,
	date '2019-02-01' as effective_date, -- this release
    1 as active,
    21000210109 as module_id,
	900000000000456007 as refset_id,
    concept_id as referenced_component_id,
    449608002 as attributeDescription,
    900000000000461009 as attribute_type,
    0 as attribute_order
from (select refset_id as concept_id from nz_simple_refset) a
	natural join (select concept_id from nz_concept where effective_date = date '2019-02-01' and active = 1) b; -- this release

insert into nz_refset_attribute ( -- attributes for new simple maps (0 rows)
	attribute_id,
	effective_date,
    active,
    module_id,
    refset_id,
    referenced_component_id,
    attribute_description,
    attribute_type,
    attribute_order
)
select uuid() as member_id,
	date '2019-02-01' as effective_date, -- this release
    1 as active,
    21000210109 as module_id,
	900000000000456007 as refset_id,
    concept_id as referenced_component_id,
    900000000000500006 as attributeDescription,
    900000000000461009 as attribute_type,
    0 as attribute_order
from (select refset_id as concept_id from nz_simple_map) a
	natural join (select concept_id, effective_date from nz_concept where effective_date = date '2019-02-01' and active = 1) b -- this release
union
select uuid() as member_id,
	date '2019-02-01' as effective_date, -- this release
    1 as active,
    21000210109 as module_id,
	900000000000456007 as refset_id,
    concept_id as referenced_component_id,
    900000000000499002 as attributeDescription,
    900000000000465000 as attribute_type,
    1 as attribute_order
from (select refset_id as concept_id from nz_simple_map) a
	natural join (select concept_id, effective_date from nz_concept where effective_date = date '2019-02-01' and active = 1) b; -- this release

select * from nz_refset_attribute
where effective_date = date '2019-02-01' -- this release
order by module_id, referenced_component_id, attribute_order;

/* Refset members */

create table nz_simple_refset_full (
	member_id char(36) not null,  
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	refset_id bigint not null, 
	concept_id bigint not null,
    primary key (member_id, effective_date)
);

load data local -- load previous release (34410 rows)
infile '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20181101T000000Z/Full/Refset/Content/der2_Refset_SimpleFull_NZ1000210_20181101.txt'
into table nz_simple_refset_full
lines terminated by '\r\n'
ignore 1 lines;

alter table nz_simple_refset_full add foreign key (module_id, effective_date) references nz_module_release (module_id, effective_date);
alter table nz_simple_refset_full add foreign key (refset_id) references nz_simple_refset (refset_id);

create table nz_simple_refset_snapshot as -- {member_id} <-> {refset_id, concept_id}
select member_id, refset_id, concept_id, effective_date, active -- 33856 members
from (
		select member_id,
			refset_id,
			concept_id,
            max(effective_date) as effective_date
		from nz_simple_refset_full
		group by member_id,
			refset_id,
            concept_id
	) a
	natural join nz_simple_refset_full
group by member_id,
	refset_id,
    concept_id;

alter table nz_simple_refset_snapshot add primary key (member_id);
alter table nz_simple_refset_snapshot add unique (refset_id, concept_id);

/* Load the new snapshot from the refset tool extracts */

create table nz_simple_refset_active (
	member_id char(36) not null,  
	effective_date date not null,
	active smallint not null,
	module_id bigint not null,
	refset_id bigint not null, 
	concept_id bigint not null,
    primary key (member_id),
    unique (refset_id, concept_id)
);

load data local
infile '/users/alastair/documents/snomed/members_disabilitySubset261000210101.txt' -- 95 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_cardiologySubset91000210107.txt' -- 63 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_emergencyCareDiagnosisSubset61000210102.txt' -- 1417 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_emergencyCarePresentingComplaintSubset71000210108.txt' -- 150 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_emergencyCareProcedureSubset321000210102.txt' -- 74 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_gynaecologySubset101000210108.txt' -- 144 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_notifiableDiseaseSubset251000210104.txt' -- 9 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_smokingSubset51000210100.txt' -- 36 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_accTranslationTableSubset81000210105.txt' -- 10106 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_rheumatologySubset121000210100.txt' -- 126 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_gatewayChildHealthAssessmentSubset241000210102.txt' -- 187 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_ambulanceClinicalImpressionSubset421000210109.txt' -- 174 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_adverseReactionManifestationSubset351000210106.txt' -- 1889 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_microorganismSubset391000210104.txt' -- 18524 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_endocrinologySubset141000210106.txt' -- 259 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_generalSurgerySubset131000210103.txt' -- 95 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_generalPaediatricSubset181000210104.txt' -- 363 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_childDevelopmentalServicesSubset191000210102.txt' -- 113 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_healthOccupationSubset451000210100.txt' -- 75 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_healthServiceType461000210102.txt' -- 72 members
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

load data local
infile '/users/alastair/documents/snomed/members_clinicalSpecialtySubset471000210108.txt' -- 50 members (the first pageful from the browser)  
into table nz_simple_refset_active
lines terminated by '\r\n'
ignore 1 lines;

select refset_id, effective_date, count(*) as n from nz_simple_refset_active group by refset_id, effective_date; -- check no null/zero dates

select * -- ideally should be empty because all concepts should be active - remove any inactives using the refset tool
from nz_simple_refset_active
	natural left outer join (select concept_id from concept) a
where a.concept_id is null
order by refset_id,
	concept_id;

-- If not empty, ie some refset members are no longer active concepts, continue but edit the refset master in the refset tool to remove/replace them

create table nz_simple_refset_active_ as -- restrict to active concepts (33833 rows)
select *
from nz_simple_refset_active
	natural join (select concept_id from concept) a;

alter table nz_simple_refset_active_ add primary key (member_id);
alter table nz_simple_refset_active_ add unique (refset_id, concept_id);
alter table nz_simple_refset_active_ add foreign key (concept_id) references concept (concept_id);

create table nz_simple_refset_del as
select member_id, refset_id, concept_id -- 268 rows
from nz_simple_refset_snapshot a
where not exists (select 1 from nz_simple_refset_active_ b where b.refset_id = a.refset_id and b.concept_id = a.concept_id)
	and active = 1;

alter table nz_simple_refset_del add primary key (member_id);
alter table nz_simple_refset_del add unique (refset_id, concept_id);

insert into nz_simple_refset_full (member_id, effective_date, active, module_id, refset_id, concept_id) -- 268 rows
select member_id,
	date '2019-02-01' as effective_date, -- this release
	0 as active,
    21000210109 as module_id,
    refset_id,
    concept_id
from nz_simple_refset_del;

create table nz_simple_refset_add as -- brand new 797 rows
select member_id, refset_id, concept_id
from nz_simple_refset_active_ a
where not exists (select 1 from nz_simple_refset_snapshot b where b.refset_id = a.refset_id and b.concept_id = a.concept_id);

alter table nz_simple_refset_add add primary key (member_id);
alter table nz_simple_refset_add add unique (refset_id, concept_id);

insert into nz_simple_refset_full (member_id, effective_date, active, module_id, refset_id, concept_id) -- 797 rows
select member_id,
	date '2019-02-01' as effective_date,
	1 as active,
    21000210109 as module_id,
    refset_id,
    concept_id
from nz_simple_refset_add;

create table nz_simple_refset_upd as -- reactivate 2 rows
select member_id, refset_id, concept_id
from nz_simple_refset_snapshot
	natural join (select refset_id, concept_id from nz_simple_refset_active_) b
where active = 0;

alter table nz_simple_refset_upd add primary key (member_id);
alter table nz_simple_refset_upd add unique (refset_id, concept_id);

insert into nz_simple_refset_full (member_id, effective_date, active, module_id, refset_id, concept_id) -- 2 rows
select member_id,
	date '2019-02-01' as effective_date,
	1 as active,
    21000210109 as module_id,
    refset_id,
    concept_id
from nz_simple_refset_upd;

create table nz_simple_refset_snapshot_final as -- {member_id} <-> {refset_id, concept_id}
select member_id, refset_id, concept_id, max(effective_date) as effective_date -- 34645 rows
from nz_simple_refset_full
group by member_id, refset_id, concept_id;

alter table nz_simple_refset_snapshot_final add primary key (member_id);
alter table nz_simple_refset_snapshot_final add unique (refset_id, concept_id);

select refset_id, concept_id, count(distinct member_id) as n -- check {refset_id, concept_id} -> {member_id} (should be empty)
from nz_simple_refset_full
group by refset_id, concept_id
having n > 1;

select member_id, count(*) as n -- check {member_id} -> {refset_id, referenced_component_id, map_target} (should be empty)
from (select distinct member_id, refset_id, concept_id from nz_simple_refset_full) a
group by member_id
having n > 1;

/* Simple refset counts */

select refset_id, count(*) as n -- total members active per refset
from nz_simple_refset_full
	natural join nz_simple_refset_snapshot_final
where active = 1
group by refset_id
order by refset_id;

select refset_id, active, count(*) as n -- members activated and inactivated per refset this release
from nz_simple_refset_full
where effective_date = date '2019-02-01' -- this release
group by refset_id, active
order by refset_id, active;

/* the end */
