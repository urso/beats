//+build !linux !cgo

package journalfield

// journaldEventFields provides default field mappings and conversions rules.
var journaldEventFields = FieldConversion{
	// provided by systemd journal
	"COREDUMP_UNIT":            text("journald.coredump.unit"),
	"COREDUMP_USER_UNIT":       text("journald.coredump.user_unit"),
	"OBJECT_AUDIT_LOGINUID":    integer("journald.object.audit.login_uid"),
	"OBJECT_AUDIT_SESSION":     integer("journald.object.audit.session"),
	"OBJECT_CMDLINE":           text("journald.object.cmd"),
	"OBJECT_COMM":              text("journald.object.name"),
	"OBJECT_EXE":               text("journald.object.executable"),
	"OBJECT_GID":               integer("journald.object.gid"),
	"OBJECT_PID":               integer("journald.object.pid"),
	"OBJECT_SYSTEMD_OWNER_UID": integer("journald.object.systemd.owner_uid"),
	"OBJECT_SYSTEMD_SESSION":   text("journald.object.systemd.session"),
	"OBJECT_SYSTEMD_UNIT":      text("journald.object.systemd.unit"),
	"OBJECT_SYSTEMD_USER_UNIT": text("journald.object.systemd.user_unit"),
	"OBJECT_UID":               integer("journald.object.uid"),
	"_KERNEL_DEVICE":           text("journald.kernel.device"),
	"_KERNEL_SUBSYSTEM":        text("journald.kernel.subsystem"),
	"_SYSTEMD_INVOCATION_ID":   text("systemd.invocation_id"),
	"_SYSTEMD_USER_SLICE":      text("systemd.user_slice"),
	"_UDEV_DEVLINK":            text("journald.kernel.device_symlinks"), // TODO aggregate multiple elements
	"_UDEV_DEVNODE":            text("journald.kernel.device_node_path"),
	"_UDEV_SYSNAME":            text("journald.kernel.device_name"),
}
