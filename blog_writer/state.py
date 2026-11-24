import operator
from typing import Annotated, List, Optional, TypedDict

from blog_writer.models import EvidenceItem, Plan


class State(TypedDict):
    topic: str

    # routing / research
    mode: str
    needs_research: bool
    queries: List[str]
    evidence: List[EvidenceItem]
    plan: Optional[Plan]

    # recency
    as_of: str
    recency_days: int

    # workers
    sections: Annotated[List[tuple[int, str]], operator.add]  # (task_id, section_md)

    # reducer/image
    merged_md: str
    md_with_placeholders: str
    image_specs: List[dict]
    review_status: str
    review_feedback: str
    revision_count: int
    revision_history: list

    final: str

    # New WordPress fields
    wordpress_post_id: Optional[int]
    wordpress_post_url: Optional[str]
    wordpress_status: Optional[str]
