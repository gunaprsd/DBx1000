#include "Row.h"
#include "row_vll.h"

#include "../system/Global.h"
#include "../system/Helper.h"

void 
Row_vll::initialize(Row * row) {
	_row = row;
	cs = 0;
	cx = 0;
}

bool 
Row_vll::insert_access(AccessType type) {
	if (type == RD) {
		cs ++;
		return (cx > 0);
	} else { 
		cx ++;
		return (cx > 1) || (cs > 0);
	}
}

void 
Row_vll::remove_access(AccessType type) {
	if (type == RD) {
		assert (cs > 0);
		cs --;
	} else {
		assert (cx > 0);
		cx --;
	}
}
