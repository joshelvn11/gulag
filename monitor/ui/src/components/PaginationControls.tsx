type PaginationControlsProps = {
  page: number;
  pageSize: number;
  receivedCount: number;
  onPageChange: (page: number) => void;
};

export function PaginationControls({
  page,
  pageSize,
  receivedCount,
  onPageChange,
}: PaginationControlsProps) {
  const canPrev = page > 0;
  const canNext = receivedCount >= pageSize;

  return (
    <div className="pager">
      <button type="button" onClick={() => onPageChange(page - 1)} disabled={!canPrev}>
        Previous
      </button>
      <span>Page {page + 1}</span>
      <button type="button" onClick={() => onPageChange(page + 1)} disabled={!canNext}>
        Next
      </button>
    </div>
  );
}
