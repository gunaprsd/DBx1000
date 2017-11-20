#include "../system/Manager.h"
#include "../system/TransactionManager.h"
#include "Row.h"
#include "row_hekaton.h"

#if CC_ALG==HEKATON

Status
TransactionManager::validate_hekaton(Status rc)
{
	uint64_t starttime = get_sys_clock();
	INC_STATS(get_thread_id(), debug1, get_sys_clock() - starttime);
	Time commit_ts = glob_manager->get_ts(get_thread_id());
	// validate the read set.
#if ISOLATION_LEVEL == SERIALIZABLE
	if (rc == OK) {
		for (uint32_t rid = 0; rid < row_cnt; rid ++) {
			if (accesses[rid]->type == WR)
				continue;
			rc = accesses[rid]->orig_row->manager->prepare_read(this, accesses[rid]->data, commit_ts);
			if (rc == ABORT)
				break;
		}
	}
#endif
	// postprocess 
	for (uint32_t rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == RD)
			continue;
		accesses[rid]->orig_row->manager->post_process(this, commit_ts, rc);
	}
	return rc;
}

#endif
