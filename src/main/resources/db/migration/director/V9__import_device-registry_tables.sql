
-- TODO Use better names

DROP TRIGGER ${old-schema}.device_hibernate_status_update
;

ALTER TABLE ${old-schema}.Device rename Device
;

ALTER TABLE ${old-schema}.DeviceHibernationStatus rename DeviceHibernationStatus
;

ALTER TABLE ${old-schema}.DeviceInstallationResult rename DeviceInstallationResult
;

ALTER TABLE ${old-schema}.EcuInstallationResult rename EcuInstallationResult
;

ALTER TABLE ${old-schema}.DeviceGroup rename DeviceGroup
;

-- still needed?
ALTER TABLE ${old-schema}.DevicePublicCredentials rename DevicePublicCredentials
;

ALTER TABLE ${old-schema}.DeviceSystem rename DeviceSystem
;

ALTER TABLE ${old-schema}.DeviceType rename DeviceType
;

-- still needed?
ALTER TABLE ${old-schema}.EcuReplacement rename EcuReplacement
;

ALTER TABLE ${old-schema}.EventJournal rename EventJournal
;

ALTER TABLE ${old-schema}.GroupMembers rename GroupMembers
;

ALTER TABLE ${old-schema}.IndexedEvents rename IndexedEvents
;

ALTER TABLE ${old-schema}.IndexedEventsArchive rename IndexedEventsArchive
;

ALTER TABLE ${old-schema}.InstalledPackage rename InstalledPackage
;

ALTER TABLE ${old-schema}.PackageListItem rename PackageListItem
;

ALTER TABLE ${old-schema}.TaggedDevice rename TaggedDevice
;

ALTER TABLE ${old-schema}.DeletedDevice rename DeletedDevice
;

CREATE TRIGGER device_hibernate_status_update AFTER UPDATE ON Device
       FOR EACH ROW
       INSERT INTO DeviceHibernationStatus (device_uuid, previous_status, new_status) VALUES (NEW.uuid, OLD.hibernated, NEW.hibernated)
;
