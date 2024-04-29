import asyncpg
import yaml, os, subprocess

def create_yaml_file():
    data = {
        'institution': f"{input('Set institution abbreviation: ')}",
        'location': f"{input('Set institution location: ')}",
    }
    filename = 'dbase_info/institution_config.yaml'
    if not os.path.exists(filename):
        with open(filename, 'w') as file:
            yaml.dump(data, file)
            print(f"Created {filename} with default values.")
          
        with open('.gitignore', 'a') as gitignore:
            gitignore.write(f"\n{filename}\n")
            print(f"Added {filename} to .gitignore.")
    else:
        print(f"{filename} already exists. Skipping creation.")

if __name__ == "__main__":
    create_yaml_file()
    subprocess.run(python, 'create/create_database.py')
    subprocess.run(python, 'create/create_tables.py')
    


#######################
### Code for host computer db creation (should only be done once, and for the first time)
### Code for host computer table creation (should only be done once, and for the first time)


#######################
### Code for Client computer (adapt based on needs of station)
