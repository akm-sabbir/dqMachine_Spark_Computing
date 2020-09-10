import dictstatus_db

# ############################
# ############################
# Dictionary DB connection
#
#
def dictionary_status_setup(_verbosity=0):
    # ######
    # Start the connection in the class
    mydb = dictstatus_db.dictstatus_db()

    # ######
    # Set the parameters
    mydb.set_hostname('lslhdpcd1.npd.com')
    mydb.set_database('PACEDICTIONARY')
    mydb.set_table('DICTIONARY_STATUS')
    mydb.set_primary_key_name('CHANGE_ID')

    # ######
    # Check we have it as we wish
    if _verbosity>0:
        mydb.get_setup()

    # ######
    # Return this for use
    return(mydb)
