# ApiCommonWebService
[WSF](https://github.com/VEuPathDB/WSF) Plugins that are common to all VEuPathDB genomics (ApiCommon) web sites.  See [WSFPlugin/src](WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin) for the list of plugins.

Additionally, the CLI implementation of the HighSpeedSnpSearch system that is called by the [High Speed Snp Search plugin](WSFPlugin/src/main/java/org/apidb/apicomplexa/wsfplugin/highspeedsnpsearch)

## Plugin Development
Plugins defined in this repository are implemented as extensions of [AbstractPlugin.java](https://github.com/VEuPathDB/WSF/blob/903ca4b3ad83a3b535b14a3f0d33462e5942f4b1/Plugin/src/main/java/org/gusdb/wsf/plugin/AbstractPlugin.java).

### Validation
In order for a 400 InvalidRequest user error to be propagated to the WDK client, a plugin's implementation of the `validate` method must throw a `PluginUserException`

By default, any exception thrown during the Plugin's `invoke` method is propagated as a 5xx error. In certain cases, it may be desired to throw a 4xx user error that can only be determined after execution of the plugin has begun. In these cases, the implementation must throw a `PostValidationUserException` which will be propagated to the client properly as a user error.

## Dependencies

   + ant
   + Perl 5
   + Java 11+
   + External dependencies: see [pom.xml](pom.xml)
   + environment variables for GUS_HOME and PROJECT_HOME
   + Internal Dependencies: see [build.xml](build.xml)

## Installation instructions.

   + bld ApiCommonWebService

## Manifest

   + WSFPlugin/config :: configuration for individual plugins
   + WSFPlugin/lib :: conifer and perl libraries for the plugins
   + WSFPlugin/src :: java source code for the plugins
   + HighSpeedSnpSearch/bin :: executables for HSSS
   + HighSpeedSnpSearch/doc :: docs for HSSS
   + HighSpeedSnpSearch/lib/perl :: perl libraries for HSSS
   + HighSpeedSnpSearch/src/c :: C src for HSSS
   + HighSpeedSnpSearch/test :: tests for HSSS
