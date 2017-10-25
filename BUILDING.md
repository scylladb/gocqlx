# BUILDING gocqlx

## Pre requisite

Go, version >= 1.8  
moreutils

### Moreutils
Need to have the `ifne` from `moreutils` packages.

Under Mac OS, just use the following command:
```
$ brew install moreutils
```
Under Linux, use your favourite package manager or the command line to retrieve this package. If you are using Arch Linux all you have to do is:
```
$ sudo pacman -S moreutils
```

Under Debian/Ubuntu/Mint a similar command is:
```
$ sudo apt install moreutils
```

And on RHEL, Fedora, CentOS:
```
$ sudo yum install moreutils
```

## Getting deps
To get all deps necessary to gocqlx you need to run the command bellow on ROOT project folder:
```
make get-deps
```

## Checking your changes
To check your code, you need to run the commands bellow on ROOT project folder:

### Check ineffectual assignments in your code.
```
make check-ineffassign
```

### Check commonly misspelled English words in source files
```
make check-misspell
```

### Check style mistakes
```
make check-lint
```

### Check format
```
make check-fmt
```

### Check all
```
make check
```

## Tests
To execute tests, you need to run the commands bellow on ROOT project folder:
### Unit tests
```
make test
```

### Integration tests
```
make integration-test
```