// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package mage

import (
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/pkg/errors"

	devtools "github.com/elastic/beats/v7/dev-tools/mage"
)

const (
	dirModuleGenerated   = "build/package/module"
	dirModulesDGenerated = "build/package/modules.d"
)

// declare journald dependencies for cross build target
var (
	journaldPlatforms = []devtools.PlatformDescription{
		devtools.Linux386, devtools.LinuxAMD64,
		devtools.LinuxARM64, devtools.LinuxARM5, devtools.LinuxARM6, devtools.LinuxARM7,
		devtools.LinuxMIPS, devtools.LinuxMIPSLE, devtools.LinuxMIPS64LE,
		devtools.LinuxPPC64LE,
		devtools.LinuxS390x,
	}

	journaldDeps = devtools.NewPackageInstaller().
			AddEach(journaldPlatforms, "libsystemd-dev").
			Add(devtools.Linux386, "libsystemd0", "libgcrypt20")
)

// CustomizePackaging modifies the package specs to add the modules and
// modules.d directory. You must declare a dependency on either
// PrepareModulePackagingOSS or PrepareModulePackagingXPack.
func CustomizePackaging() {
	var (
		moduleTarget = "module"
		module       = devtools.PackageFile{
			Mode:   0644,
			Source: dirModuleGenerated,
		}

		modulesDTarget = "modules.d"
		modulesD       = devtools.PackageFile{
			Mode:    0644,
			Source:  dirModulesDGenerated,
			Config:  true,
			Modules: true,
		}
	)

	for _, args := range devtools.Packages {
		for _, pkgType := range args.Types {
			switch pkgType {
			case devtools.TarGz, devtools.Zip, devtools.Docker:
				args.Spec.Files[moduleTarget] = module
				args.Spec.Files[modulesDTarget] = modulesD
			case devtools.Deb, devtools.RPM:
				args.Spec.Files["/usr/share/{{.BeatName}}/"+moduleTarget] = module
				args.Spec.Files["/etc/{{.BeatName}}/"+modulesDTarget] = modulesD
			case devtools.DMG:
				args.Spec.Files["/Library/Application Support/{{.BeatVendor}}/{{.BeatName}}/"+moduleTarget] = module
				args.Spec.Files["/etc/{{.BeatName}}/"+modulesDTarget] = modulesD
			default:
				panic(errors.Errorf("unhandled package type: %v", pkgType))
			}
			break
		}
	}
}

// PrepareModulePackagingOSS generates build/package/modules and
// build/package/modules.d directories for use in packaging.
func PrepareModulePackagingOSS() error {
	return prepareModulePackaging([]struct{ Src, Dst string }{
		{devtools.OSSBeatDir("module"), dirModuleGenerated},
		{devtools.OSSBeatDir("modules.d"), dirModulesDGenerated},
	}...)
}

// PrepareModulePackagingXPack generates build/package/modules and
// build/package/modules.d directories for use in packaging.
func PrepareModulePackagingXPack() error {
	return prepareModulePackaging([]struct{ Src, Dst string }{
		{devtools.OSSBeatDir("module"), dirModuleGenerated},
		{"module", dirModuleGenerated},
		{devtools.OSSBeatDir("modules.d"), dirModulesDGenerated},
		{"modules.d", dirModulesDGenerated},
	}...)
}

// prepareModulePackaging generates build/package/modules and
// build/package/modules.d directories for use in packaging.
func prepareModulePackaging(files ...struct{ Src, Dst string }) error {
	// This depends on the modules.d directory being up-to-date.
	mg.Deps(devtools.GenerateDirModulesD)

	// Clean any existing generated directories.
	if err := devtools.Clean([]string{dirModuleGenerated, dirModulesDGenerated}); err != nil {
		return err
	}

	for _, copyAction := range files {
		err := (&devtools.CopyTask{
			Source:  copyAction.Src,
			Dest:    copyAction.Dst,
			Mode:    0644,
			DirMode: 0755,
			Exclude: []string{
				"/_meta",
				"/test",
				"fields.go",
			},
		}).Execute()
		if err != nil {
			return err
		}
	}
	return nil
}

// GolangCrossBuild build the Beat binary inside of the golang-builder.
// Do not use directly, use crossBuild instead.
func GolangCrossBuild() error {
	mg.Deps(BuildDepsInstaller(devtools.Platform.Name))
	return devtools.GolangCrossBuild(devtools.DefaultGolangCrossBuildArgs())
}

func CrossBuildXPack() error {
	return devtools.CrossBuildXPack(devtools.ImageSelector(SelectCrossBuildImage))
}

// CrossBuild cross-builds the beat for all target platforms.
func CrossBuild() error {
	return devtools.CrossBuild(devtools.ImageSelector(SelectCrossBuildImage))
}

// CrossBuildGoDaemon cross-builds the go-daemon binary using Docker.
func CrossBuildGoDaemon() error {
	return devtools.CrossBuildGoDaemon(devtools.ImageSelector(SelectCrossBuildImage))
}

func BuildDepsInstaller(platform string) func() error {
	return journaldDeps.Installer(platform)
}

func SelectCrossBuildImage(platform string) (string, error) {
	tagSuffix := "main"

	switch {
	case strings.HasPrefix(platform, "linux/arm"):
		tagSuffix = "arm"
	case strings.HasPrefix(platform, "linux/mips"):
		tagSuffix = "mips"
	case strings.HasPrefix(platform, "linux/ppc"):
		tagSuffix = "ppc"
	case platform == "linux/s390x":
		tagSuffix = "s390x"
	case strings.HasPrefix(platform, "linux"):
		tagSuffix = "main-debian8"
	}

	goVersion, err := devtools.GoVersion()
	if err != nil {
		return "", err
	}

	return devtools.BeatsCrossBuildImage + ":" + goVersion + "-" + tagSuffix, nil
}
