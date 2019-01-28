#!/usr/bin/perl
#
# Query MySQL SNOMED NZ Extension database and write RF2 files

use strict;
use warnings;
use v5.10; # for say() function
use DBI;

print "nz_extension_dump_jan2019 running ...";
# MySQL database configuration
my $username = "alastair";
my $password = 'Brainfade93';
 
# connect to MySQL database
my %attr = (
	PrintError=>0, # turn off error reporting via warn()
	RaiseError=>1	 # turn on error reporting via die()
);
 
my $dbh	= DBI->connect("DBI:mysql:snomed_jan2019", $username, $password, \%attr);
 
print "Connected to MySQL database\n";

my $filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Refset/Metadata/der2_ssRefset_ModuleDependencyFull_NZ1000210_20190201.txt';
open(my $fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	sourceEffectiveTime	targetEffectiveTime\r\n");
my $sql = "
	select dep_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
	   	refset_id,
		referenced_component_id,
	    date_format(source_effective_date, '%Y%m%d') as source_effective_date,
	    date_format(target_effective_date, '%Y%m%d') as target_effective_date
	from nz_module_dep
	order by module_id, effective_date, referenced_component_id, target_effective_date";
my $sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7]);
}			 
$sth->finish();
close $fh;

$filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Refset/Metadata/der2_ssRefset_ModuleDependencyDelta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	sourceEffectiveTime	targetEffectiveTime\r\n");
$sql = "
	select dep_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
	    refset_id,
		referenced_component_id,
	    date_format(source_effective_date, '%Y%m%d') as source_effective_date,
	    date_format(target_effective_date, '%Y%m%d') as target_effective_date
	from nz_module_dep
	where effective_date = date '2019-02-01' -- this release
	order by module_id, effective_date, referenced_component_id, target_effective_date";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7]);
}			 
$sth->finish();
close $fh;

$filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Refset/Metadata/der2_ssRefset_ModuleDependencySnapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	sourceEffectiveTime	targetEffectiveTime\r\n");
$sql = "
	select dep_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
	    refset_id,
		referenced_component_id,
	    date_format(source_effective_date, '%Y%m%d') as source_effective_date,
	    date_format(target_effective_date, '%Y%m%d') as target_effective_date
	from nz_module_dep_snapshot
		natural join nz_module_dep
	order by module_id, effective_date, referenced_component_id, target_effective_date";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Terminology/sct2_Concept_Delta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	definitionStatusId\r\n");
$sql = "
	select concept_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		definition_status_id
	from nz_concept
	where effective_date = date '2019-02-01' -- this release
	order by concept_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Terminology/sct2_Concept_Snapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	definitionStatusId\r\n");
$sql = "
	select concept_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		definition_status_id
	from nz_concept_snapshot
		natural join nz_concept
	order by concept_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Terminology/sct2_Description_Delta-en-NZ_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	conceptId	languageCode	typeId	term	caseSignificanceId\r\n");
$sql = "
	select description_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		concept_id,
	    language_code,
	    type_id,
	    term,
	    case_significance_id
	from nz_description
	where effective_date = date '2019-02-01' -- this release
	order by description_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8]);
}			 
$sth->finish();
close $fh;


$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Terminology/sct2_Description_Snapshot-en-NZ_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	conceptId	languageCode	typeId	term	caseSignificanceId\r\n");
$sql = "
	select description_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		concept_id,
	    language_code,
	    type_id,
	    term,
	    case_significance_id
	from nz_description_snapshot
		natural join nz_description
	order by description_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Terminology/sct2_Relationship_Delta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	sourceId	destinationId	relationshipGroup	typeId	characteristicTypeId	modifierId\r\n");
$sql = "
	select relationship_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		source_id,
		destination_id,
		relationship_group,
		type_id,
		characteristic_type_id,
		modifier_id
	from nz_relationship
	where effective_date = date '2019-02-01' -- this release
	order by relationship_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8], $row[9]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Terminology/sct2_Relationship_Snapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	sourceId	destinationId	relationshipGroup	typeId	characteristicTypeId	modifierId\r\n");
$sql = "
	select relationship_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		source_id,
		destination_id,
		relationship_group,
		type_id,
		characteristic_type_id,
		modifier_id
	from nz_relationship_snapshot
		natural join nz_relationship
	order by relationship_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8], $row[9]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Terminology/sct2_StatedRelationship_Delta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	sourceId	destinationId	relationshipGroup	typeId	characteristicTypeId	modifierId\r\n");
$sql = "
	select relationship_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		source_id,
		destination_id,
		relationship_group,
		type_id,
		characteristic_type_id,
		modifier_id
	from nz_stated_relationship
	where effective_date = date '2019-02-01' -- this release
	order by relationship_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8], $row[9]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Terminology/sct2_StatedRelationship_Snapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	sourceId	destinationId	relationshipGroup	typeId	characteristicTypeId	modifierId\r\n");
$sql = "
	select relationship_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		source_id,
		destination_id,
		relationship_group,
		type_id,
		characteristic_type_id,
		modifier_id
	from nz_stated_relationship_snapshot
		natural join nz_stated_relationship
	order by relationship_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8], $row[9]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Refset/Map/der2_sRefset_SimpleMapFull_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	mapTarget\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		referenced_component_id,
	    map_target
	from nz_simple_map_full
	order by member_id, effective_date";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Refset/Map/der2_sRefset_SimpleMapDelta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	mapTarget\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		referenced_component_id,
	    map_target
	from nz_simple_map_full
	where effective_date = date '2019-02-01' -- this release
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Refset/Map/der2_sRefset_SimpleMapSnapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	mapTarget\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		referenced_component_id,
	    map_target
	from nz_simple_map_full
		natural join nz_simple_map_snapshot
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Refset/Language/der2_cRefset_LanguageFull-en-NZ_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	acceptabilityId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
	    refset_id, 
		description_id as referenced_component_id,
		acceptability_id
	from nz_en_lang_refset
	order by member_id, effective_date";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Refset/Language/der2_cRefset_LanguageDelta-en-NZ_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	acceptabilityId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
	    refset_id,
		description_id as referenced_component_id,
		acceptability_id
	from nz_en_lang_refset
	where effective_date = date '2019-02-01' -- this release
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;


$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Refset/Language/der2_cRefset_LanguageSnapshot-en-NZ_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	acceptabilityId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		21000210109 as module_id,
	    refset_id,
		description_id as referenced_component_id,
		acceptability_id
	from nz_en_lang_refset_snapshot_final
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6]);
}			 
$sth->finish();
close $fh;

$filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Refset/Metadata/der2_cciRefset_RefsetDescriptorFull_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	attributeDescription	attributeType	attributeOrder\r\n");
$sql = "
	select attribute_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id, -- 21000210109
		refset_id, -- 900000000000456007
		referenced_component_id, -- the refset or map in the extension
		attribute_description,
		attribute_type,
		attribute_order
	from nz_refset_attribute
	order by module_id, effective_date, referenced_component_id, attribute_order";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8]);
}			 
$sth->finish();
close $fh;

$filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Refset/Metadata/der2_cciRefset_RefsetDescriptorDelta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	attributeDescription	attributeType	attributeOrder\r\n");
$sql = "
	select attribute_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id, -- 21000210109
		refset_id, -- 900000000000456007
		referenced_component_id, -- the refset or map in the extension
		attribute_description,
		attribute_type,
		attribute_order
	from nz_refset_attribute
	where effective_date = date '2019-02-01' -- this release 
	order by module_id, effective_date, referenced_component_id, attribute_order";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8]);
}			 
$sth->finish();
close $fh;

$filename = '/Users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Refset/Metadata/der2_cciRefset_RefsetDescriptorSnapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId	attributeDescription	attributeType	attributeOrder\r\n");
$sql = "
	select attribute_id, -- assumes no changes for the moment
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id, -- 21000210109
		refset_id, -- 900000000000456007
		referenced_component_id, -- the refset or map in the extension
		attribute_description,
		attribute_type,
		attribute_order
	from nz_refset_attribute
	order by module_id, effective_date, referenced_component_id, attribute_order";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5], $row[6], $row[7], $row[8]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Full/Refset/Content/der2_Refset_SimpleFull_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		concept_id
	from nz_simple_refset_full
	order by member_id, effective_date";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Delta/Refset/Content/der2_Refset_SimpleDelta_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		concept_id
	from nz_simple_refset_full
	where effective_date = date '2019-02-01' -- this release
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5]);
}			 
$sth->finish();
close $fh;

$filename = '/users/alastair/documents/snomed/SnomedCT_NZExtensionRF2_BETA_20190201T000000Z/Snapshot/Refset/Content/der2_Refset_SimpleSnapshot_NZ1000210_20190201.txt';
open($fh, '>', $filename) or die "Could not open file '$filename' $!";
printf($fh "id	effectiveTime	active	moduleId	refsetId	referencedComponentId\r\n");
$sql = "
	select member_id,
		date_format(effective_date, '%Y%m%d') as effective_date,
		active,
		module_id,
		refset_id,
		concept_id
	from nz_simple_refset_full
		natural join nz_simple_refset_snapshot_final
	order by member_id";
$sth = $dbh->prepare($sql);
$sth->execute();
while(my @row = $sth->fetchrow_array()) {
	printf($fh "%s	%s	%s	%s	%s	%s\r\n", $row[0], $row[1], $row[2], $row[3], $row[4], $row[5]);
}			 
$sth->finish();
close $fh;

$dbh->disconnect();

print " done\n";

# the end
