#ifndef ROW_VLL_H
#define ROW_VLL_H

class Row_vll {
public:
	void init(Row * row);
	// return true   : the access is blocked.
	// return false	 : the access is NOT blocked 
	bool insert_access(AccessType type);
	void remove_access(AccessType type);
	int get_cs() { return cs; };
private:
	Row * _row;
    int cs;
    int cx;
};

#endif
