#!/bin/sh

####
# Usage ./migrate_shard.sh sourceDB destDB ttl_offset
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

echo -e "Increasing TTLS on $1 by $3\n"
python adjust_ttl.py --source $1/marconi_messages_p0/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p1/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p2/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p3/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p4/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p5/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p6/messages --seconds $3
echo -e "\n"
python adjust_ttl.py --source $1/marconi_messages_p7/messages --seconds $3
echo -e "\n"

echo -e "Copying Queues from $1 to $2\n"
python copy_collection.py --source $1/marconi_queues/queues --dest $2/marconi_queues/queues

echo -e "\nCopying nprojects from $1 to $2 \n"
python copy_collection.py --source $1/marconi_queues/nprojects --dest $2/marconi_queues/nprojects

echo -e "\nCopying Messages_0 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p0/messages --dest $2/marconi_messages_p0/messages
echo -e "\nCopying Messages_1 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p1/messages --dest $2/marconi_messages_p1/messages
echo -e "\nCopying Messages_2 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p2/messages --dest $2/marconi_messages_p2/messages
echo -e "\nCopying Messages_3 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p3/messages --dest $2/marconi_messages_p3/messages
echo -e  "\nCopying Messages_4 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p4/messages --dest $2/marconi_messages_p4/messages
echo -e "\nCopying Messages_5 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p5/messages --dest $2/marconi_messages_p5/messages
echo -e "\nCopying Messages_6 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p6/messages --dest $2/marconi_messages_p6/messages
echo -e "\nCopying Messages_7 from $1 to $2 \n"
python copy_collection.py --source $1/marconi_messages_p7/messages --dest $2/marconi_messages_p7/messages
echo -e "\nCopying Messages_8 from $1 to $2 \n"

echo -e "\nPerforming Integrity Checks on Messages_0\n"
python compare_collections.py --source $1/marconi_messages_p0/messages --dest $2/marconi_messages_p0/messages
echo -e "\nPerforming Integrity Checks on Messages_1\n"
python compare_collections.py --source $1/marconi_messages_p1/messages --dest $2/marconi_messages_p1/messages
echo -e "\nPerforming Integrity Checks on Messages_2\n"
python compare_collections.py --source $1/marconi_messages_p2/messages --dest $2/marconi_messages_p2/messages
echo -e "\nPerforming Integrity Checks on Messages_3\n"
python compare_collections.py --source $1/marconi_messages_p3/messages --dest $2/marconi_messages_p3/messages
echo -e "\nPerforming Integrity Checks on Messages_4\n"
python compare_collections.py --source $1/marconi_messages_p4/messages --dest $2/marconi_messages_p4/messages
echo -e "\nPerforming Integrity Checks on Messages_5\n"
python compare_collections.py --source $1/marconi_messages_p5/messages --dest $2/marconi_messages_p5/messages
echo -e "\nPerforming Integrity Checks on Messages_6\n"
python compare_collections.py --source $1/marconi_messages_p6/messages --dest $2/marconi_messages_p6/messages
echo -e "\nPerforming Integrity Checks on Messages_7\n"
python compare_collections.py --source $1/marconi_messages_p7/messages --dest $2/marconi_messages_p7/messages

echo -e "\nPerforming Integrity Checks on Queues\n"
python compare_collections.py --source $1/marconi_queues/queues --dest $2/marconi_queues/queues

echo -e "\nCopying nprojects from $1 to $2 \n"
python compare_collections.py --source $1/marconi_queues/nprojects --dest $2/marconi_queues/nprojects

echo -e "\nCleaning up...."

cleanup

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Migration completed in $DIFF seconds"


#Change connection string
