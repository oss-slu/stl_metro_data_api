# src/read_service/api/building_permits.py

from flask import Blueprint, request, jsonify
from sqlalchemy import text
from .rate_limit import rate_limit_stub

def create_building_permits_blueprint(SessionLocal):
    """
    Factory that creates the building permits blueprint with access
    to SessionLocal for DB queries.

    Endpoint:
        GET /api/building

    Query Parameters:
        page (int, optional): 1-based page number. Default = 1.
        page_size (int, optional): Page size. Default = 50, max = 200.

    Behavior:
        - Filters to rows where is_active = TRUE.
        - Orders by data_posted_on DESC, id ASC.
        - Returns paginated JSON with metadata + results list.
    """
    bp = Blueprint("building_permits", __name__, url_prefix="/api")

    @bp.route("/building", methods=["GET"])
    @rate_limit_stub(max_requests=100, window_seconds=60)
    def get_building_permits():
        """
        Get paginated active building permit records from Postgres.

        This reads from the stlouis_building_permit table, which is
        populated by the write_service ingestion pipeline (web scraper).

        Returns:
            JSON response:
            {
                "page": int,
                "page_size": int,
                "total": int,
                "results": [ ... ]
            }
        """
        # --- pagination params ---
        try:
            page = int(request.args.get("page", 1))
        except ValueError:
            page = 1

        try:
            page_size = int(request.args.get("page_size", 50))
        except ValueError:
            page_size = 50

        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 1
        if page_size > 200:
            page_size = 200

        offset = (page - 1) * page_size

        db = SessionLocal()

        # total count of active rows
        total_result = db.execute(
            text("SELECT COUNT(*) FROM stlouis_building_permit WHERE is_active = TRUE")
        )
        total = total_result.scalar_one()

        # page of active rows
        rows = db.execute(
            text("""
                SELECT
                    id,
                    neighborhood,
                    total_permits,
                    total_value,
                    avg_days_to_issue,
                    created_on,
                    data_posted_on,
                    is_active
                FROM stlouis_building_permit
                WHERE is_active = TRUE
                ORDER BY data_posted_on DESC, id ASC
                LIMIT :limit OFFSET :offset
            """),
            {"limit": page_size, "offset": offset},
        )

        results = []
        for row in rows:
            m = row._mapping
            results.append({
                "id": m["id"],
                "neighborhood": m["neighborhood"],
                "total_permits": m["total_permits"],
                "total_value": float(m["total_value"]) if m["total_value"] is not None else None,
                "avg_days_to_issue": m["avg_days_to_issue"],
                "created_on": m["created_on"].isoformat() if m["created_on"] else None,
                "data_posted_on": m["data_posted_on"].isoformat() if m["data_posted_on"] else None,
                "is_active": m["is_active"],
            })

        db.close()

        return jsonify({
            "page": page,
            "page_size": page_size,
            "total": total,
            "results": results,
        }), 200

    return bp
