"""Activity registries, grouped by the worker that runs them."""

from shopsphere.activities.inventory import (
    create_purchase_orders,
    get_low_stock_skus,
    release_inventory,
    reserve_inventory,
)
from shopsphere.activities.notifications import send_notification
from shopsphere.activities.orders import award_loyalty_points, save_order_snapshot
from shopsphere.activities.payments import charge_payment, refund_payment
from shopsphere.activities.shipping import create_shipping_label
from shopsphere.activities.support_tools import (
    check_return_eligibility,
    create_return_label,
    get_order_details,
    issue_refund,
    lookup_return_policy,
)

ORDER_ACTIVITIES = [
    reserve_inventory,
    release_inventory,
    charge_payment,
    refund_payment,
    create_shipping_label,
    send_notification,
    save_order_snapshot,
    award_loyalty_points,
    get_low_stock_skus,
    create_purchase_orders,
]

SUPPORT_TOOL_ACTIVITIES = [
    get_order_details,
    check_return_eligibility,
    create_return_label,
    issue_refund,
    lookup_return_policy,
]
