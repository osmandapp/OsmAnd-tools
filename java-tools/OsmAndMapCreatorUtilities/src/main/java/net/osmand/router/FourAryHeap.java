package net.osmand.router;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;


public class FourAryHeap<T> implements Queue<T> {
    private List<T> heap;
	private Comparator<T> cmp;

    public FourAryHeap(Comparator<T> cmp) {
        this.cmp = cmp;
		heap = new ArrayList<>();
    }

    // Insert an element into the heap
    public void insert(T element) {
        heap.add(element);
        int currentIndex = heap.size() - 1;
        int parentIndex = (currentIndex - 1) / 4;

        while (currentIndex > 0 && cmp.compare(heap.get(currentIndex), heap.get(parentIndex)) < 0) {
            swap(currentIndex, parentIndex);
            currentIndex = parentIndex;
            parentIndex = (currentIndex - 1) / 4;
        }
    }

    // Extract the minimum element (root) from the heap
    public T extractMin() {
        if (isEmpty()) {
            throw new IllegalStateException("Heap is empty");
        }

        T min = heap.get(0);
        int lastIndex = heap.size() - 1;
        heap.set(0, heap.get(lastIndex));
        heap.remove(lastIndex);
        heapify(0);

        return min;
    }

    // Heapify the heap starting at the given index
    private void heapify(int index) {
        int minIndex = index;
        int childIndex;

        for (int i = 1; i <= 4; i++) {
            childIndex = 4 * index + i;
            if (childIndex < heap.size() && cmp.compare(heap.get(childIndex), heap.get(minIndex)) < 0) {
                minIndex = childIndex;
            }
        }

        if (minIndex != index) {
            swap(index, minIndex);
            heapify(minIndex);
        }
    }

    // Check if the heap is empty
    public boolean isEmpty() {
        return heap.isEmpty();
    }

    // Swap two elements in the heap
    private void swap(int i, int j) {
        T temp = heap.get(i);
        heap.set(i, heap.get(j));
        heap.set(j, temp);
    }

    // Get the size of the heap
    public int size() {
        return heap.size();
    }

    public static void main(String[] args) {
        FourAryHeap<Integer> heap = new FourAryHeap<>(new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				return Integer.compare(o1, o2);
			}
		});
        heap.insert(10);
        heap.insert(5);
        heap.insert(15);
        heap.insert(3);

        System.out.println("Min Element: " + heap.extractMin()); // Should print 3
    }

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(T e) {
		insert(e);
		return true;
	}

	@Override
	public boolean offer(T e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public T poll() {
		return extractMin();
	}

	@Override
	public T element() {
		throw new UnsupportedOperationException();
	}

	@Override
	public T peek() {
		throw new UnsupportedOperationException();
		
	}
}