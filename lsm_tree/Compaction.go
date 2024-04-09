package lsm_tree

import (
	"container/heap"
	"dmds_lab2/shared"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    []byte
	arrIndex int // The index of the array from which the item was taken.
	valIndex int // The index of the item within its array.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the smallest, not largest, value so we use less than here.
	k1, err := shared.ByteToKey(pq[i].value[:shared.KeySize])
	if err != nil {
		panic(err)
	}

	k2, err := shared.ByteToKey(pq[j].value[:shared.KeySize])
	if err != nil {
		panic(err)
	}

	return k1 < k2
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Item)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func RemoveTombstones(arrays [][]byte) ([][]byte, error) {
	result := make([][]byte, len(arrays))

	for arrIndex, arr := range arrays {
		for startIndex := uint64(0); startIndex < uint64(len(arr)); startIndex += shared.BlockSize {
			endIndex := startIndex + shared.BlockSize
			_, value, err := shared.ByteToKeyValue(arr[startIndex:endIndex])
			if err != nil {
				return nil, err
			}

			if !shared.IsTombstone(value) {
				result[arrIndex] = append(result[arrIndex], arr[startIndex:endIndex]...)
			}
		}
	}

	return result, nil
}

// MergeSortedArray merges K sorted arrays of byte slices into a single sorted array.
// Ref: https://en.wikipedia.org/wiki/K-way_merge_algorithm
func MergeSortedArray(arrays [][]byte) ([]byte, error) {
	if len(arrays) < 1 {
		return []byte{}, nil
	}
	if len(arrays) == 1 {
		return arrays[0], nil
	}

	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	result := []byte{}

	// Initialize the priority queue with the first element of each array.
	for i, arr := range arrays {
		if len(arr) > 0 {
			heap.Push(&pq, &Item{value: arr[:shared.BlockSize], arrIndex: i, valIndex: 0})
		}
	}

	// While the priority queue is not empty, extract the minimum element and add the next element of that array to the heap.
	for pq.Len() > 0 {
		// Extract the minimum element and add the next element of that array to the heap.
		item := heap.Pop(&pq).(*Item)

		result = append(result, item.value...)

		// If there are more elements in the array, add the next element to the heap.
		if uint64(item.valIndex)+1 < uint64(len(arrays[item.arrIndex]))/shared.BlockSize {
			start := int(shared.BlockSize) * (item.valIndex + 1)
			end := int(shared.BlockSize) * (item.valIndex + 2)
			nextItem := &Item{
				value:    arrays[item.arrIndex][start:end],
				arrIndex: item.arrIndex,
				valIndex: item.valIndex + 1,
			}
			heap.Push(&pq, nextItem)
		}
	}

	return result, nil
}

func MergeDuplicatedKeys(arrays [][]byte) [][]byte {
	existingKeys := make(map[shared.KeyType]struct{})
	result := make([][]byte, len(arrays))

	for arrIndex, arr := range arrays {
		for startIndex := uint64(0); startIndex < uint64(len(arr)); startIndex += shared.BlockSize {
			endIndex := startIndex + shared.BlockSize
			key, _, err := shared.ByteToKeyValue(arr[startIndex:endIndex])
			if err != nil {
				panic(err)
			}

			if _, ok := existingKeys[key]; ok {
				existingKeys[key] = struct{}{}
			} else {
				existingKeys[key] = struct{}{}
				result[arrIndex] = append(result[arrIndex], arr[startIndex:endIndex]...)
			}
		}
	}

	return result
}
