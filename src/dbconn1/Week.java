package dbconn1;

import java.util.Comparator;

public class Week  implements Comparator<Week>{
	int weekNo;

	public int getWeekNo() {
		return weekNo;
	}

	public void setWeekNo(int weekNo) {
		this.weekNo = weekNo;
	}
	
	@Override
	public int compare(Week arg0,Week arg1) {

        if (arg0.getWeekNo() < arg1.getWeekNo()) {System.out.println("returning -1");return -1;}
        if (arg0.getWeekNo() > arg1.getWeekNo()){ System.out.println("returning 1");return 1;}
        return 0;
          
    }
	
}
