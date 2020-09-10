import dictstatus_db

# ############################
# ############################
# Dictionary DB connection
# 
# set up the parameter for database connectivity
# return the newly initiated database object
def dictionary_status_setup(_verbosity=0,tab_name ='DICTIONARY_STATUS'):
    # ######
    # Start the connection in the class
    mydb = dictstatus_db.dictstatus_db()

    # ######
    # Set the parameters
    mydb.set_hostname('lslhdpcd1.npd.com')
    mydb.set_database('PACEDICTIONARY')
    mydb.set_table(tab_name)
    mydb.set_primary_key_name('CHANGE_ID')

    # ######
    # Check we have it as we wish
    if _verbosity > 0:
        mydb.get_setup()

    # ######
    # Return this for use
    return(mydb)
