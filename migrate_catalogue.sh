#!/bin/sh

####
# Usage ./migrate_catalogue.sh sourceDB destDB
####

cleanup()
{
	rm --f marconi_messages_p0.messages.db
	rm --f marconi_messages_p1.messages.db
	rm --f marconi_messages_p2.messages.db
	rm --f marconi_messages_p3.messages.db
	rm --f marconi_messages_p4.messages.db
	rm --f marconi_messages_p5.messages.db
	rm --f marconi_messages_p6.messages.db
	rm --f marconi_messages_p7.messages.db
	rm --f marconi_queues.queues.db
	rm --f marconi_catalogue.catalogue.db
	rm --f marconi_shards.shards.db
	rm --f marconi_queues.nprojects.db

}

control_c()
# run if user hits control-c
{
  echo -en "\n*** Ouch! Exiting ***\n"
  cleanup
  exit $?
}
 
# trap keyboard interrupt (control-c)
trap control_c SIGINT

START=$(date +%s)

echo -e "\nCopying Catalogue from $1 to $2 \n"
python copy_collection.py --source $1/marconi_catalogue/catalogue --dest $2/marconi_catalogue/catalogue

echo -e "\nCopying Shards from $1 to $2 \n"
python copy_collection.py --source $1/marconi_shards/shards --dest $2/marconi_shards/shards

echo -e "\nPerforming Integrity Checks on Catalogue\n"
python compare_collections.py --source $1/marconi_catalogue/catalogue --dest $2/marconi_catalogue/catalogue

echo -e "\nPerforming Integrity Checks on Shards\n"
python compare_collections.py --source $1/marconi_shards/shards --dest $2/marconi_shards/shards

echo -e "\nCleaning up...."

cleanup

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Migration completed in $DIFF seconds"


#Change connection string
