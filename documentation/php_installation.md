# Install `PHP` to use browser-based Adminer-Evo

### Linux Ubuntu/Debian
```
sudo apt update && sudo apt upgrade -y
sudo apt install php -y
php -v
```

### MacOS using Homebrew
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
php -v
```

### Windows
- [Download XAMPP](https://www.apachefriends.org/download.html)
- Install XAMPP and deselect the installation of all additional non-essential packages. Ensure PHP and Apache is selected. Check under `C:\xampp\php` that `php.exe` exists.
- After installation is complete, open `Edit system environment variable` Control Panel window. Select `Environment Variables...`. Under `System variables`, navigate to find the `Path` variable. Double-click to edit `Path`. In the pop-up window, add `C:\xampp\php` to the end of the list. Save and exit.
- Open a new Powershell terminal. Type and enter `php -v` to check installation was successful.


The same can be done for Linux and MacOS. 
For MacOS, add `php` to path in `zshrc` (or `bashrc`).
```
export PATH="/Applications/XAMPP/xamppfiles/bin:$PATH"
```