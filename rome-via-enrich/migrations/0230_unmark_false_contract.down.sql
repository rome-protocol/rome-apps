-- Not reversible: we cannot know which of the un-marked rows were originally
-- true by mistake vs. correct. Re-running the old broken predicate would just
-- re-introduce the false positives, so the down is a no-op.
SELECT 1;
