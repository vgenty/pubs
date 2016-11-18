yes | ../sbin/remove_all_projects 

yes | ../sbin/remove_runtable DummyRunTable
yes | ../sbin/remove_runtable FragmentRunTable_seb01
# yes | ../sbin/remove_runtable FragmentRunTable_seb02
# yes | ../sbin/remove_runtable FragmentRunTable_seb03
# yes | ../sbin/remove_runtable FragmentRunTable_seb04
# yes | ../sbin/remove_runtable FragmentRunTable_seb05
# yes | ../sbin/remove_runtable FragmentRunTable_seb06
# yes | ../sbin/remove_runtable FragmentRunTable_seb07
# yes | ../sbin/remove_runtable FragmentRunTable_seb08
# yes | ../sbin/remove_runtable FragmentRunTable_seb09


yes | ../sbin/create_runtable DummyRunTable
yes | ../sbin/create_runtable FragmentRunTable_seb01
# yes | ../sbin/create_runtable FragmentRunTable_seb02
# yes | ../sbin/create_runtable FragmentRunTable_seb03
# yes | ../sbin/create_runtable FragmentRunTable_seb04
# yes | ../sbin/create_runtable FragmentRunTable_seb05
# yes | ../sbin/create_runtable FragmentRunTable_seb06
# yes | ../sbin/create_runtable FragmentRunTable_seb07
# yes | ../sbin/create_runtable FragmentRunTable_seb08
# yes | ../sbin/create_runtable FragmentRunTable_seb09


yes | ../sbin/register_project register_snova.cfg
yes | ../sbin/register_daemon ../daemon.cfg