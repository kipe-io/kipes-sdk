---
label: Release Workflow
authors:
 - name: Jens Günther
   link: https://github.com/jens-guenther
layout: page
order: -1
visibility: hidden

---

# Release Workflow

**NOTE**  

You have to be an authorized maintainer before you can release :) 
Talk to us if you are interested! Get in contact with [maintainers@kipe.io].

## Release Setup
  
### Configuring Maven settings.xml
  
Showing only relevant settings:  
  
```xml

  <!-- configure your sonatype JIRA credentials registered with the relevant project -->
  <server>
    <id>ossrh</id>
    <username>username</username>
    <password>password</password>
  </server>
  
  <!-- configure git access -->
  <server>
    <id>github</id>
    <username>git</username>
    <privateKey>path-to-ssh-private-key</privateKey>
  </server>
  
  <profiles>

    <!-- configure the needed release profile settings -->
    <profile>
      <id>release-kipe</id>
      <activation>
        <activeByDefault>false</activeByDefault>
      </activation>
      <properties>
        <project.scm.id>github</project.scm.id>
        <gpg.passphrase><![CDATA[your-gpg-key-passphrase]]></gpg.passphrase>
      </properties>
    </profile>
  </profiles>
```

## Release Execution

### 1. Release Preparation

- block main from further PRs
- checkout release branch from main
- verify version numbers: RC Release vs. Official Release

### 2. Run Staging Release to OSS


- run through the typical maven release process

```
$ mvn -Prelease-kipe release:prepare
$ mvn -Prelease-kipe release:perform
```

on Nexus https://s01.oss.sonatype.org/#stagingRepositories

- check the staging repository if everything looks good
- release or drop accordingly

### Cleanup After Dropping a Release

- `mvn -Prelease-kipe release:clean`
- drop the last two commits done by the maven-release-plugin and force-push
- delete the release tag locally and on Github
- unblock main to allow new PRs

### Finalizing a Successful Release

- `mvn -Prelease-kipe release:clean`
- merge (not rebase) the release branch to main
- unblock main to allow new PRs

