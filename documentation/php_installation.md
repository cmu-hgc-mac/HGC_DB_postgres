# Install `PHP` to use browser-based Adminer-Evo

## Linux Ubuntu/Debian
```
sudo apt install php -y
sudo apt install php-pgsql php-pdo-pgsql
php -v
```

## MacOS using Homebrew
- Install Homebrew
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew --version
```
- Or update Homebrew
```
brew update
brew upgrade
```
- Install PHP and verify
```
brew install php
brew install php-pgsql
php -v
```
Find `php.ini` and enable
```
extension=pgsql
extension=pdo_pgsql
```


## Windows
- [Download XAMPP](https://www.apachefriends.org/download.html)
- Install XAMPP and deselect the installation of all additional non-essential packages. Ensure PHP and Apache is selected. Check under `C:\xampp\php` that `php.exe` exists after installation is complete.
- Open `Edit system environment variable` Control Panel window. Select `Environment Variables...`. Under `System variables`, navigate to find the `Path` variable. Double-click to edit `Path`. In the pop-up window, add `C:\xampp\php` to the end of the list. Save and exit.
- Open `php.ini` under `C:\xampp\php\` and comment out the following lines:
```
extension=pgsql
extension=pdo_pgsql
```
- Open a new Powershell terminal. Type and enter `php -v` to check installation was successful.

The same may be done for Linux and MacOS if not using the above mentioned methods but we haven't tested these.
For MacOS, add `php` to path in `zshrc` (or `bashrc`).
```
export PATH="/Applications/XAMPP/xamppfiles/bin:$PATH"
```
