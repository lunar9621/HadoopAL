package apriori;

import java.util.Collections;
import java.util.List;

public class Transaction {
	private int tid;
	private int cid;
	private List<Integer> items;

	public Transaction(int tid, int cid, List<Integer> items) {
		super();
		this.tid = tid;
		this.cid = cid;
		setItems(items);
	}

	@Override
	public String toString() {
		return "Transaction [tid=" + tid + ", cid=" + cid + ", items=" + items + "]";
	}

	public int getTid() {
		return tid;
	}

	public void setTid(int tid) {
		this.tid = tid;
	}

	public int getCid() {
		return cid;
	}

	public void setCid(int cid) {
		this.cid = cid;
	}

	public List<Integer> getItems() {
		return items;
	}

	public void setItems(List<Integer> items) {
		this.items = items;
		// Keep the list of items in a transaction sorted. This aids in the
		// candidate generation
		// and pruning phase.
		Collections.sort(this.items);
	}

}
