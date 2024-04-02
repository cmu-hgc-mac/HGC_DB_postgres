import pwinput
user_input = None
def get_user_input():
    global user_input
    if user_input is None:
        user_input = pwinput.pwinput(prompt='Enter user password: ', mask='*')
    return user_input

host='cmsmac04.phys.cmu.edu'
database='hgcdb'
user='postgres'
# password='hgcal'
password=get_user_input()
