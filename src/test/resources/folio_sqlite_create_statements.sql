CREATE TABLE `holdingFolio` ( `id` varchar(37) NOT NULL,  `hrid` varchar(12) NOT NULL,  `instanceId` varchar(37) NOT NULL,  `instanceHrid` varchar(12) NOT NULL,  `active` integer  NOT NULL,  `moddate` timestamp NULL DEFAULT NULL,  `content` longtext,  `podCurrent` integer DEFAULT '0',  UNIQUE (`id`),  UNIQUE (`hrid`) );
CREATE INDEX "idx_holdingFolio_instanceHrid" ON "holdingFolio" (`instanceHrid`);
CREATE INDEX "idx_holdingFolio_podCurrent" ON "holdingFolio" (`podCurrent`);
CREATE INDEX "idx_holdingFolio_instanceId" ON "holdingFolio" (`instanceId`);

CREATE TABLE `itemFolio` (  `id` varchar(37) NOT NULL,  `hrid` varchar(12) NOT NULL,  `holdingId` varchar(37) NOT NULL,  `holdingHrid` varchar(12) NOT NULL,  `sequence` integer  DEFAULT NULL,  `barcode` varchar(15) DEFAULT NULL,  `moddate` timestamp NULL DEFAULT NULL,  `content` longtext,  UNIQUE (`hrid`),  UNIQUE (`id`));
CREATE INDEX "idx_itemFolio_holdingHrid" ON "itemFolio" (`holdingHrid`);
CREATE INDEX "idx_itemFolio_barcode" ON "itemFolio" (`barcode`);
CREATE INDEX "idx_itemFolio_holdingId" ON "itemFolio" (`holdingId`);

CREATE TABLE `itemSequence` (  `hrid` varchar(12) NOT NULL, `sequence` integer DEFAULT NULL);
CREATE INDEX "idx_itemSequence_hrid" ON "itemSequence" (`hrid`);

CREATE TABLE `itemDueDates` (  `bib_id` integer NOT NULL, `change_date` timestamp NULL DEFAULT NULL, json TEXT);
CREATE INDEX "idx_itemDueDates_bib_id" ON "itemDueDates" (`bib_id`);

CREATE TABLE `itemRequests` (  `bib_id` integer NOT NULL, `change_date` timestamp NULL DEFAULT NULL, json TEXT);
CREATE INDEX "idx_itemRequests_bib_id" ON "itemRequests" (`bib_id`);

CREATE TABLE `loanFolio` (  `id` varchar(37) NOT NULL,  `holdingId` varchar(37) NOT NULL,  `itemHrid` varchar(12) NOT NULL,  `moddate` timestamp NULL DEFAULT NULL,  `content` longtext DEFAULT NULL)

CREATE TABLE `classification` (`low_letters` char(3) NOT NULL collate nocase, `high_letters` char(3) NOT NULL collate nocase,  `low_numbers` float(9,4) NOT NULL,  `high_numbers` float(9,4) NOT NULL,  `label` varchar(256) NOT NULL )

