from __future__ import annotations

import os
import re
import calendar
import json
import base64
import hashlib
import secrets
import hmac
import struct
import statistics
from io import BytesIO
from functools import wraps
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Optional
from urllib.parse import quote_plus

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, send_file, send_from_directory, session, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, inspect, text
from sqlalchemy.exc import OperationalError
from werkzeug.security import check_password_hash, generate_password_hash

def normalize_database_url(raw_url: Optional[str]) -> Optional[str]:
    if not raw_url:
        return None
    url = raw_url.strip()
    if not url:
        return None
    if url.startswith('mysql://'):
        return f"mysql+pymysql://{url[len('mysql://'):]}"
    return url


def build_database_url() -> str:
    # Railway often provides one of these URL-style variables depending on plugin/service type.
    url_candidates = (
        os.getenv('DATABASE_URL'),
        os.getenv('MYSQL_URL'),
        os.getenv('MYSQL_PUBLIC_URL'),
        os.getenv('RAILWAY_DATABASE_URL'),
    )
    for candidate in url_candidates:
        normalized = normalize_database_url(candidate)
        if normalized:
            return normalized

    # Fallback to component variables (Railway MySQL plugin style + generic DB_* aliases).
    mysql_host = os.getenv('MYSQLHOST') or os.getenv('DB_HOST')
    mysql_user = os.getenv('MYSQLUSER') or os.getenv('DB_USER')
    mysql_password = os.getenv('MYSQLPASSWORD') or os.getenv('DB_PASSWORD') or ''
    mysql_port = os.getenv('MYSQLPORT') or os.getenv('DB_PORT') or '3306'
    mysql_db = os.getenv('MYSQLDATABASE') or os.getenv('DB_NAME')

    if mysql_host and mysql_user and mysql_db:
        auth_part = mysql_user
        if mysql_password:
            auth_part = f'{mysql_user}:{quote_plus(mysql_password)}'
        return f'mysql+pymysql://{auth_part}@{mysql_host}:{mysql_port}/{mysql_db}'

    instance_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'instance')
    os.makedirs(instance_dir, exist_ok=True)
    sqlite_path = os.path.join(instance_dir, 'money_manager.db').replace('\\', '/')
    return f'sqlite:///{sqlite_path}'


app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-change-me')
app.config['SQLALCHEMY_DATABASE_URI'] = build_database_url()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_pre_ping': True}
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

db = SQLAlchemy(app)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    account_type = db.Column(db.String(50), nullable=False, default='Cash')
    opening_balance = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    currency = db.Column(db.String(10), nullable=False, default='INR')
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    user = db.relationship('User', backref='accounts')
    transactions = db.relationship('Transaction', backref='account', lazy=True)


class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    kind = db.Column(db.String(20), nullable=False, default='expense')
    icon = db.Column(db.String(100), nullable=False, default='bi bi-tag')
    color = db.Column(db.String(20), nullable=False, default='#6c757d')
    is_active = db.Column(db.Boolean, nullable=False, default=True)

    transactions = db.relationship('Transaction', backref='category', lazy=True)


class Transaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tx_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    tx_type = db.Column(db.String(20), nullable=False, default='expense')
    notes = db.Column(db.Text)
    payee = db.Column(db.String(120))
    source = db.Column(db.String(50), nullable=False, default='manual')
    reference_no = db.Column(db.String(120))
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    user = db.relationship('User')


class TransactionMeta(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    transaction_id = db.Column(db.Integer, db.ForeignKey('transaction.id'), unique=True, nullable=False, index=True)
    label = db.Column(db.String(80))
    payment_type = db.Column(db.String(40))
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    transaction = db.relationship('Transaction', backref=db.backref('tx_meta', uselist=False))


class RecurringTransaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(255), nullable=False)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    tx_type = db.Column(db.String(20), nullable=False, default='expense')
    payee = db.Column(db.String(120))
    notes = db.Column(db.Text)
    frequency = db.Column(db.String(20), nullable=False, default='monthly')
    interval_value = db.Column(db.Integer, nullable=False, default=1)
    next_run_date = db.Column(db.Date, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class Bill(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    bill_kind = db.Column(db.String(30), nullable=False, default='credit_card')
    amount_due = db.Column(db.Numeric(12, 2), nullable=False)
    minimum_due = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    annual_interest_rate = db.Column(db.Numeric(8, 2), nullable=False, default=0)
    outstanding_balance = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    next_due_date = db.Column(db.Date, nullable=False)
    notes = db.Column(db.Text)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class BillPayment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    bill_id = db.Column(db.Integer, db.ForeignKey('bill.id'), nullable=False)
    payment_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    amount_paid = db.Column(db.Numeric(12, 2), nullable=False)
    interest_component = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    principal_component = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    notes = db.Column(db.Text)

    bill = db.relationship('Bill', backref='payments')


class InvestmentAsset(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(30), unique=True, nullable=False)
    name = db.Column(db.String(120), nullable=False)
    asset_class = db.Column(db.String(30), nullable=False, default='stock')  # stock / mutual_fund / etf / bond / other
    currency = db.Column(db.String(10), nullable=False, default='INR')
    last_price = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    last_price_at = db.Column(db.Date)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class InvestmentTransaction(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    asset_id = db.Column(db.Integer, db.ForeignKey('investment_asset.id'), nullable=False)
    tx_date = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    tx_kind = db.Column(db.String(20), nullable=False, default='buy')  # buy / sell / dividend / sip
    quantity = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    unit_price = db.Column(db.Numeric(14, 4), nullable=False, default=0)
    fees = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    notes = db.Column(db.Text)
    source = db.Column(db.String(40), nullable=False, default='manual')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    asset = db.relationship('InvestmentAsset', backref='transactions')


class MonthlyBudget(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    month_key = db.Column(db.String(7), nullable=False)  # YYYY-MM
    category_id = db.Column(db.Integer, db.ForeignKey('category.id'), nullable=False)
    amount_limit = db.Column(db.Numeric(12, 2), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    category = db.relationship('Category')
    user = db.relationship('User')


class SavingsGoal(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    target_amount = db.Column(db.Numeric(12, 2), nullable=False)
    current_saved = db.Column(db.Numeric(12, 2), nullable=False, default=0)
    target_date = db.Column(db.Date)
    is_completed = db.Column(db.Boolean, nullable=False, default=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    user = db.relationship('User')


class SecurityToken(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    purpose = db.Column(db.String(40), nullable=False)  # email_verify / password_reset
    token_hash = db.Column(db.String(128), nullable=False, index=True)
    expires_at = db.Column(db.DateTime, nullable=False)
    used_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    user = db.relationship('User')


class UserSecurity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), unique=True, nullable=False)
    email_verified = db.Column(db.Boolean, nullable=False, default=False)
    two_factor_enabled = db.Column(db.Boolean, nullable=False, default=False)
    two_factor_secret_enc = db.Column(db.Text)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = db.relationship('User')


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    action = db.Column(db.String(120), nullable=False)
    details = db.Column(db.Text)
    ip_address = db.Column(db.String(60))
    user_agent = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    user = db.relationship('User')


ICON_OPTIONS = [
    ('bi bi-cart', 'Cart'),
    ('bi bi-house-door', 'House'),
    ('bi bi-receipt-cutoff', 'Bills'),
    ('bi bi-car-front', 'Car'),
    ('bi bi-fuel-pump', 'Fuel'),
    ('bi bi-bag', 'Shopping Bag'),
    ('bi bi-cup-hot', 'Food / Tea'),
    ('bi bi-egg-fried', 'Meal'),
    ('bi bi-airplane', 'Travel'),
    ('bi bi-train-front', 'Train'),
    ('bi bi-heart-pulse', 'Medical'),
    ('bi bi-lightning-charge', 'Utilities'),
    ('bi bi-phone', 'Mobile'),
    ('bi bi-laptop', 'Laptop'),
    ('bi bi-bank', 'Bank'),
    ('bi bi-cash-stack', 'Cash'),
    ('bi bi-wallet2', 'Wallet'),
    ('bi bi-graph-up-arrow', 'Investment'),
    ('bi bi-briefcase', 'Salary'),
    ('bi bi-arrow-left-right', 'Transfer'),
    ('bi bi-gift', 'Gift'),
    ('bi bi-person', 'Personal'),
    ('bi bi-tag', 'Generic'),
]

CATEGORY_STYLE_MAP = {
    'bills': ('bi bi-receipt-cutoff', '#0d6efd'),
    'groceries': ('bi bi-cart', '#0d6efd'),
    'rent': ('bi bi-house-door', '#fd7e14'),
    'utilities': ('bi bi-lightning-charge', '#6f42c1'),
    'transport': ('bi bi-car-front', '#198754'),
    'dining': ('bi bi-cup-hot', '#dc3545'),
    'medical': ('bi bi-heart-pulse', '#e83e8c'),
    'shopping': ('bi bi-bag', '#20c997'),
    'salary': ('bi bi-briefcase', '#198754'),
    'freelance': ('bi bi-laptop', '#0dcaf0'),
    'investment': ('bi bi-graph-up-arrow', '#6610f2'),
    'transfer': ('bi bi-arrow-left-right', '#6c757d'),
}


def is_placeholder_color(color_value: Optional[str]) -> bool:
    if not color_value:
        return True
    normalized = color_value.strip().lower()
    return normalized in {'#6c757d', '#6c757dff', 'rgb(108,117,125)', 'rgba(108,117,125,1)'}


DATE_COLUMN_ALIASES = ['Date', 'Transaction Date', 'Txn Date', 'Value Date', 'Posting Date']
DESC_COLUMN_ALIASES = [
    'Description',
    'Details',
    'Narration',
    'Merchant',
    'Transaction Details',
    'Transaction Detail',
    'Transaction Description',
    'Particulars',
]
AMOUNT_COLUMN_ALIASES = [
    'Amount',
    'Amount (INR)',
    'Amount(in Rs)',
    'Amount (in Rs)',
    'Amount (Rs)',
    'Transaction Amount',
    'Txn Amount',
    'Debit',
    'Credit',
]
TYPE_COLUMN_ALIASES = ['Type', 'Dr/Cr', 'Debit/Credit', 'Transaction Type', 'Cr/Dr']
CATEGORY_COLUMN_ALIASES = ['Category']
REFERENCE_COLUMN_ALIASES = ['Reference Number', 'Reference No', 'Ref No']


def seed_defaults() -> None:
    default_categories = [
        ('Bills', 'expense', 'bi bi-receipt-cutoff', '#0d6efd'),
        ('Groceries', 'expense', 'bi bi-cart', '#0d6efd'),
        ('Rent', 'expense', 'bi bi-house-door', '#fd7e14'),
        ('Utilities', 'expense', 'bi bi-lightning-charge', '#6f42c1'),
        ('Transport', 'expense', 'bi bi-car-front', '#198754'),
        ('Dining', 'expense', 'bi bi-cup-hot', '#dc3545'),
        ('Medical', 'expense', 'bi bi-heart-pulse', '#e83e8c'),
        ('Shopping', 'expense', 'bi bi-bag', '#20c997'),
        ('Salary', 'income', 'bi bi-briefcase', '#198754'),
        ('Freelance', 'income', 'bi bi-laptop', '#0dcaf0'),
        ('Investment', 'transfer', 'bi bi-graph-up-arrow', '#6610f2'),
        ('Transfer', 'transfer', 'bi bi-arrow-left-right', '#6c757d'),
    ]

    for name, kind, icon, color in default_categories:
        exists = Category.query.filter_by(name=name).first()
        if not exists:
            db.session.add(Category(name=name, kind=kind, icon=icon, color=color))
        else:
            if not exists.icon or exists.icon.strip() == 'bi bi-tag':
                exists.icon = icon
            if is_placeholder_color(exists.color):
                exists.color = color

    # Backfill placeholders for known category names even if case/spacing differs.
    for category in Category.query.all():
        mapped = CATEGORY_STYLE_MAP.get(category.name.strip().lower())
        if not mapped:
            continue
        mapped_icon, mapped_color = mapped
        if not category.icon or category.icon.strip() == 'bi bi-tag':
            category.icon = mapped_icon
        if is_placeholder_color(category.color):
            category.color = mapped_color

    db.session.commit()


def ensure_ownership_schema_updates() -> None:
    inspector = inspect(db.engine)
    account_columns = {col['name'] for col in inspector.get_columns('account')}
    transaction_columns = {col['name'] for col in inspector.get_columns('transaction')}
    budget_columns = {col['name'] for col in inspector.get_columns('monthly_budget')}
    goal_columns = {col['name'] for col in inspector.get_columns('savings_goal')}
    statements: list[str] = []
    if 'user_id' not in account_columns:
        statements.append('ALTER TABLE account ADD COLUMN user_id INTEGER')
    if 'user_id' not in transaction_columns:
        statements.append('ALTER TABLE `transaction` ADD COLUMN user_id INTEGER')
    if 'user_id' not in budget_columns:
        statements.append('ALTER TABLE monthly_budget ADD COLUMN user_id INTEGER')
    if 'user_id' not in goal_columns:
        statements.append('ALTER TABLE savings_goal ADD COLUMN user_id INTEGER')
    if not statements:
        return
    with db.engine.begin() as connection:
        for sql in statements:
            try:
                connection.execute(text(sql))
            except OperationalError as exc:
                # Multiple workers can race on startup; ignore duplicate-column in that case.
                if 'Duplicate column name' in str(exc):
                    continue
                raise


def run_ownership_schema_updates_safely() -> bool:
    try:
        ensure_ownership_schema_updates()
        return True
    except Exception:
        db.session.rollback()
        app.logger.exception('Failed to apply ownership schema updates.')
        return False


def current_user_id() -> Optional[int]:
    value = session.get('user_id')
    return int(value) if isinstance(value, int) else None


def accounts_for_user_query(user_id: Optional[int] = None):
    target_user_id = user_id if user_id is not None else current_user_id()
    return Account.query.filter(Account.user_id == target_user_id)


def transactions_for_user_query(user_id: Optional[int] = None):
    target_user_id = user_id if user_id is not None else current_user_id()
    return Transaction.query.filter(Transaction.user_id == target_user_id)


def next_available_account_name(base_name: str, user_id: int) -> str:
    candidate = base_name
    serial = 1
    while Account.query.filter(func.lower(Account.name) == candidate.lower()).first():
        suffix = f'({user_id})' if serial == 1 else f'({user_id}-{serial})'
        candidate = f'{base_name} {suffix}'
        serial += 1
    return candidate


def ensure_user_workspace(user_id: int) -> None:
    if not user_id:
        return
    changed = False
    if accounts_for_user_query(user_id).count() == 0:
        orphan_accounts = Account.query.filter(Account.user_id.is_(None)).order_by(Account.id.asc()).all()
        if orphan_accounts:
            for account in orphan_accounts:
                account.user_id = user_id
            changed = True
        else:
            db.session.add(
                Account(
                    name=next_available_account_name('Cash Wallet', user_id),
                    account_type='Cash',
                    opening_balance=0,
                    currency='INR',
                    user_id=user_id,
                )
            )
            db.session.add(
                Account(
                    name=next_available_account_name('Primary Bank', user_id),
                    account_type='Bank',
                    opening_balance=0,
                    currency='INR',
                    user_id=user_id,
                )
            )
            changed = True

    account_ids = [row[0] for row in accounts_for_user_query(user_id).with_entities(Account.id).all()]
    if account_ids:
        updated = (
            Transaction.query
            .filter(Transaction.account_id.in_(account_ids), Transaction.user_id.is_(None))
            .update({'user_id': user_id}, synchronize_session=False)
        )
        if updated:
            changed = True

    if SavingsGoal.query.filter_by(user_id=user_id).count() == 0:
        claimed_goals = (
            SavingsGoal.query
            .filter(SavingsGoal.user_id.is_(None))
            .update({'user_id': user_id}, synchronize_session=False)
        )
        if claimed_goals:
            changed = True

    if MonthlyBudget.query.filter_by(user_id=user_id).count() == 0:
        claimed_budgets = (
            MonthlyBudget.query
            .filter(MonthlyBudget.user_id.is_(None))
            .update({'user_id': user_id}, synchronize_session=False)
        )
        if claimed_budgets:
            changed = True

    if changed:
        db.session.commit()


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if session.get('user_id'):
            return view_func(*args, **kwargs)
        flash('Please login to continue.', 'warning')
        return redirect(url_for('login', next=request.path))

    return wrapper


@app.context_processor
def inject_auth_context():
    user_id = session.get('user_id')
    email_verified = False
    two_factor_enabled = False
    if user_id:
        security = UserSecurity.query.filter_by(user_id=user_id).first()
        if security:
            email_verified = bool(security.email_verified)
            two_factor_enabled = bool(security.two_factor_enabled)

    return {
        'is_authenticated': bool(user_id),
        'auth_user_name': session.get('user_name', ''),
        'auth_email_verified': email_verified,
        'auth_two_factor_enabled': two_factor_enabled,
    }


def month_key_for(target_date: date) -> str:
    return target_date.strftime('%Y-%m')


def ensure_user_security(user_id: int) -> UserSecurity:
    security = UserSecurity.query.filter_by(user_id=user_id).first()
    if security:
        return security
    security = UserSecurity(user_id=user_id)
    db.session.add(security)
    db.session.commit()
    return security


def log_audit(action: str, details: str = '') -> None:
    try:
        entry = AuditLog(
            user_id=session.get('user_id'),
            action=action,
            details=details or None,
            ip_address=request.remote_addr,
            user_agent=(request.user_agent.string[:255] if request.user_agent and request.user_agent.string else None),
        )
        db.session.add(entry)
        db.session.commit()
    except Exception:
        db.session.rollback()


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode('utf-8')).hexdigest()


def create_security_token(user_id: int, purpose: str, ttl_minutes: int = 30) -> str:
    raw_token = secrets.token_urlsafe(32)
    token = SecurityToken(
        user_id=user_id,
        purpose=purpose,
        token_hash=hash_token(raw_token),
        expires_at=datetime.utcnow() + timedelta(minutes=ttl_minutes),
    )
    db.session.add(token)
    db.session.commit()
    return raw_token


def consume_security_token(raw_token: str, purpose: str) -> Optional[SecurityToken]:
    token_hash = hash_token(raw_token)
    token = (
        SecurityToken.query
        .filter_by(token_hash=token_hash, purpose=purpose)
        .order_by(SecurityToken.id.desc())
        .first()
    )
    if not token:
        return None
    if token.used_at is not None:
        return None
    if token.expires_at < datetime.utcnow():
        return None

    token.used_at = datetime.utcnow()
    db.session.commit()
    return token


def _secret_key_bytes() -> bytes:
    return hashlib.sha256((app.config['SECRET_KEY'] + '|money-manager').encode('utf-8')).digest()


def encrypt_secret(plain_text: str) -> str:
    data = plain_text.encode('utf-8')
    key = _secret_key_bytes()
    nonce = secrets.token_bytes(12)
    out = bytearray()
    counter = 0
    while len(out) < len(data):
        counter_bytes = struct.pack('>I', counter)
        block = hmac.new(key, nonce + counter_bytes, hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    cipher = bytes(a ^ b for a, b in zip(data, out[:len(data)]))
    mac = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(nonce + mac + cipher).decode('utf-8')


def decrypt_secret(cipher_text: str) -> str:
    payload = base64.urlsafe_b64decode(cipher_text.encode('utf-8'))
    nonce, mac, cipher = payload[:12], payload[12:44], payload[44:]
    key = _secret_key_bytes()
    expected_mac = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected_mac):
        raise ValueError('Secret verification failed')
    out = bytearray()
    counter = 0
    while len(out) < len(cipher):
        counter_bytes = struct.pack('>I', counter)
        block = hmac.new(key, nonce + counter_bytes, hashlib.sha256).digest()
        out.extend(block)
        counter += 1
    plain = bytes(a ^ b for a, b in zip(cipher, out[:len(cipher)]))
    return plain.decode('utf-8')


def generate_totp_secret() -> str:
    return base64.b32encode(secrets.token_bytes(20)).decode('utf-8').rstrip('=')


def _totp_code(secret: str, for_time: int) -> str:
    normalized = secret.upper() + '=' * ((8 - len(secret) % 8) % 8)
    key = base64.b32decode(normalized, casefold=True)
    counter = int(for_time // 30)
    msg = struct.pack('>Q', counter)
    digest = hmac.new(key, msg, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code_int = ((digest[offset] & 0x7F) << 24) | ((digest[offset + 1] & 0xFF) << 16) | ((digest[offset + 2] & 0xFF) << 8) | (digest[offset + 3] & 0xFF)
    return str(code_int % 1000000).zfill(6)


def verify_totp_code(secret: str, code: str, window: int = 1) -> bool:
    clean_code = str(code or '').strip()
    if not clean_code.isdigit():
        return False
    now = int(datetime.utcnow().timestamp())
    for shift in range(-window, window + 1):
        if _totp_code(secret, now + shift * 30) == clean_code:
            return True
    return False


def add_months(base_date: date, months: int) -> date:
    year = base_date.year + (base_date.month - 1 + months) // 12
    month = (base_date.month - 1 + months) % 12 + 1
    day = min(base_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def get_next_occurrence(next_date: date, frequency: str, interval_value: int) -> date:
    interval_value = max(1, int(interval_value or 1))
    if frequency == 'weekly':
        return next_date + timedelta(weeks=interval_value)
    return add_months(next_date, interval_value)


def process_recurring_transactions(run_date: Optional[date] = None, user_id: Optional[int] = None) -> int:
    target_date = run_date or datetime.utcnow().date()
    recurring_query = (
        RecurringTransaction.query
        .join(Account, RecurringTransaction.account_id == Account.id)
        .filter(RecurringTransaction.is_active.is_(True))
        .filter(RecurringTransaction.next_run_date <= target_date)
    )
    if user_id is not None:
        recurring_query = recurring_query.filter(Account.user_id == user_id)
    recurring_items = recurring_query.all()

    created_count = 0
    changed = False
    for item in recurring_items:
        safety = 0
        while item.next_run_date and item.next_run_date <= target_date and safety < 60:
            reference_key = f'rec-{item.id}-{item.next_run_date.isoformat()}'
            exists = Transaction.query.filter_by(
                source='recurring',
                reference_no=reference_key,
                user_id=user_id,
            ).first()
            if not exists:
                tx_date = datetime.combine(item.next_run_date, datetime.min.time())
                transaction = Transaction(
                    tx_date=tx_date,
                    description=item.description.strip() or item.name.strip(),
                    amount=Decimal(item.amount),
                    tx_type=item.tx_type,
                    notes=item.notes or 'Auto-posted recurring transaction',
                    payee=item.payee,
                    source='recurring',
                    reference_no=reference_key,
                    account_id=item.account_id,
                    category_id=item.category_id,
                    user_id=user_id,
                )
                db.session.add(transaction)
                created_count += 1
                changed = True

            item.next_run_date = get_next_occurrence(item.next_run_date, item.frequency, item.interval_value)
            changed = True
            safety += 1

    if changed:
        db.session.commit()
    return created_count


def bill_interest_estimate(bill: Bill) -> Decimal:
    outstanding = Decimal(bill.outstanding_balance or 0)
    annual_rate = Decimal(bill.annual_interest_rate or 0)
    monthly_rate = annual_rate / Decimal('1200')
    return (outstanding * monthly_rate).quantize(Decimal('0.01'))


def build_payment_planner(bills: list[Bill], as_of: date) -> list[dict]:
    planner = []
    for bill in bills:
        overdue_days = max((as_of - bill.next_due_date).days, 0)
        minimum_due = Decimal(bill.minimum_due or 0)
        amount_due = Decimal(bill.amount_due or 0)
        interest = bill_interest_estimate(bill)
        recommended = max(minimum_due, amount_due) + (interest if overdue_days > 0 else Decimal('0'))
        planner.append(
            {
                'bill': bill,
                'overdue_days': overdue_days,
                'interest_estimate': interest,
                'recommended_payment': recommended,
            }
        )
    return planner


def normalize_asset_class(value: str) -> str:
    cleaned = str(value or '').strip().lower()
    aliases = {
        'stock': 'stock',
        'equity': 'stock',
        'share': 'stock',
        'mutual_fund': 'mutual_fund',
        'mutualfund': 'mutual_fund',
        'mf': 'mutual_fund',
        'fund': 'mutual_fund',
        'etf': 'etf',
        'bond': 'bond',
    }
    return aliases.get(cleaned, 'other')


def normalize_investment_kind(value: str) -> str:
    cleaned = str(value or '').strip().lower()
    aliases = {
        'buy': 'buy',
        'purchase': 'buy',
        'sip': 'sip',
        'sell': 'sell',
        'redeem': 'sell',
        'redemption': 'sell',
        'dividend': 'dividend',
        'payout': 'dividend',
    }
    return aliases.get(cleaned, 'buy')


def resolve_investment_column(df: pd.DataFrame, preferred: str, aliases: list[str]) -> Optional[str]:
    if preferred and preferred in df.columns:
        return preferred

    normalized_to_actual = {}
    for col in df.columns:
        normalized_to_actual[normalize_import_key(col)] = col

    if preferred:
        normalized_preferred = normalize_import_key(preferred)
        if normalized_preferred in normalized_to_actual:
            return normalized_to_actual[normalized_preferred]

    for alias in aliases:
        normalized_alias = normalize_import_key(alias)
        if normalized_alias in normalized_to_actual:
            return normalized_to_actual[normalized_alias]

    return None


def calculate_portfolio_snapshot() -> dict:
    assets = InvestmentAsset.query.order_by(InvestmentAsset.symbol.asc()).all()
    realized_total = Decimal('0')
    total_cost = Decimal('0')
    total_value = Decimal('0')
    holdings = []

    for asset in assets:
        txs = (
            InvestmentTransaction.query
            .filter_by(asset_id=asset.id)
            .order_by(InvestmentTransaction.tx_date.asc(), InvestmentTransaction.id.asc())
            .all()
        )
        qty = Decimal('0')
        cost_open = Decimal('0')
        realized = Decimal('0')

        for tx in txs:
            tx_qty = Decimal(tx.quantity or 0)
            tx_price = Decimal(tx.unit_price or 0)
            tx_fees = Decimal(tx.fees or 0)
            kind = normalize_investment_kind(tx.tx_kind)

            if kind in {'buy', 'sip'}:
                if tx_qty <= 0:
                    continue
                qty += tx_qty
                cost_open += (tx_qty * tx_price) + tx_fees
            elif kind == 'sell':
                if tx_qty <= 0 or qty <= 0:
                    continue
                sell_qty = min(tx_qty, qty)
                avg_cost = (cost_open / qty) if qty > 0 else Decimal('0')
                removed_cost = avg_cost * sell_qty
                proceeds = (sell_qty * tx_price) - tx_fees
                realized += proceeds - removed_cost
                qty -= sell_qty
                cost_open -= removed_cost
                if qty <= 0:
                    qty = Decimal('0')
                    cost_open = Decimal('0')
            elif kind == 'dividend':
                gross = (tx_qty * tx_price) if tx_qty > 0 else tx_price
                realized += gross - tx_fees

        market_price = Decimal(asset.last_price or 0)
        market_value = qty * market_price
        unrealized = market_value - cost_open

        if qty > 0 or realized != 0:
            holdings.append(
                {
                    'asset': asset,
                    'quantity': qty,
                    'cost_open': cost_open,
                    'market_value': market_value,
                    'unrealized': unrealized,
                    'realized': realized,
                    'return_pct': float((unrealized / cost_open) * Decimal('100')) if cost_open > 0 else 0.0,
                }
            )

        realized_total += realized
        total_cost += cost_open
        total_value += market_value

    holdings.sort(key=lambda row: row['market_value'], reverse=True)

    allocation_labels = [row['asset'].symbol for row in holdings if row['market_value'] > 0]
    allocation_values = [float(row['market_value']) for row in holdings if row['market_value'] > 0]
    returns_labels = [row['asset'].symbol for row in holdings]
    returns_values = [float(row['unrealized']) for row in holdings]

    class_map = {}
    for row in holdings:
        class_name = row['asset'].asset_class
        class_map[class_name] = class_map.get(class_name, Decimal('0')) + row['market_value']
    class_labels = list(class_map.keys())
    class_values = [float(class_map[label]) for label in class_labels]

    return {
        'holdings': holdings,
        'total_cost': total_cost,
        'total_value': total_value,
        'total_unrealized': total_value - total_cost,
        'total_realized': realized_total,
        'allocation_labels': allocation_labels,
        'allocation_values': allocation_values,
        'class_labels': class_labels,
        'class_values': class_values,
        'returns_labels': returns_labels,
        'returns_values': returns_values,
    }


@app.before_request
def run_recurring_engine():
    public_endpoints = {
        None,
        'static',
        'login',
        'signup',
        'login_two_factor',
        'request_password_reset',
        'password_reset',
        'verify_email',
    }
    if request.endpoint in public_endpoints:
        return

    if session.get('pending_2fa_user_id') and not session.get('user_id'):
        allowed = {'login_two_factor', 'logout'}
        if request.endpoint not in allowed:
            return redirect(url_for('login_two_factor'))

    if not session.get('user_id'):
        return
    user_id = current_user_id()
    if not user_id:
        return
    try:
        ensure_user_workspace(user_id)
    except OperationalError:
        db.session.rollback()
        if not run_ownership_schema_updates_safely():
            return
        ensure_user_workspace(user_id)

    today_key = datetime.utcnow().date().isoformat()
    if session.get('last_recurring_run') == today_key:
        return

    try:
        created = process_recurring_transactions(user_id=user_id)
    except OperationalError:
        db.session.rollback()
        if not run_ownership_schema_updates_safely():
            return
        created = process_recurring_transactions(user_id=user_id)
    session['last_recurring_run'] = today_key
    if created > 0:
        flash(f'Auto-posted {created} recurring transaction(s).', 'info')


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if session.get('user_id'):
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        try:
            name = request.form.get('name', '').strip()
            email = request.form.get('email', '').strip().lower()
            password = request.form.get('password', '')
            confirm_password = request.form.get('confirm_password', '')

            if not name or not email or not password:
                flash('Name, email, and password are required.', 'warning')
                return render_template('signup.html')

            if password != confirm_password:
                flash('Password and confirm password do not match.', 'warning')
                return render_template('signup.html')

            exists = User.query.filter(func.lower(User.email) == email).first()
            if exists:
                flash('An account with this email already exists.', 'warning')
                return render_template('signup.html')

            user = User(
                name=name,
                email=email,
                password_hash=generate_password_hash(password),
            )
            db.session.add(user)
            db.session.commit()

            session['user_id'] = user.id
            session['user_name'] = user.name
            ensure_user_workspace(user.id)
            ensure_user_security(user.id)
            verify_token = create_security_token(user.id, 'email_verify', ttl_minutes=60 * 24)
            verification_link = url_for('verify_email', token=verify_token, _external=False)
            flash('Signup successful. Welcome!', 'success')
            flash(f'Verify your email: {verification_link}', 'info')
            log_audit('auth.signup', f'user={user.email}')
            return redirect(url_for('dashboard'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not create account: {exc}', 'danger')

    return render_template('signup.html')


@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('user_id'):
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')
        user = User.query.filter(func.lower(User.email) == email).first()

        if not user or not check_password_hash(user.password_hash, password):
            flash('Invalid email or password.', 'danger')
            return render_template('login.html')

        security = ensure_user_security(user.id)
        next_path = request.form.get('next', '').strip() or request.args.get('next', '').strip()

        if security.two_factor_enabled:
            session['pending_2fa_user_id'] = user.id
            session['pending_2fa_user_name'] = user.name
            session['pending_2fa_next'] = next_path if next_path.startswith('/') else ''
            flash('Enter your 2FA code to finish login.', 'info')
            return redirect(url_for('login_two_factor'))

        session['user_id'] = user.id
        session['user_name'] = user.name
        ensure_user_workspace(user.id)
        session.pop('pending_2fa_user_id', None)
        session.pop('pending_2fa_user_name', None)
        session.pop('pending_2fa_next', None)
        flash('Login successful.', 'success')
        log_audit('auth.login', f'user={user.email}')
        if next_path.startswith('/'):
            return redirect(next_path)
        return redirect(url_for('dashboard'))

    return render_template('login.html')


@app.route('/login/2fa', methods=['GET', 'POST'])
def login_two_factor():
    pending_user_id = session.get('pending_2fa_user_id')
    if not pending_user_id:
        return redirect(url_for('login'))

    user = User.query.get(pending_user_id)
    if not user:
        session.pop('pending_2fa_user_id', None)
        session.pop('pending_2fa_user_name', None)
        session.pop('pending_2fa_next', None)
        return redirect(url_for('login'))

    security = ensure_user_security(user.id)
    if not security.two_factor_enabled or not security.two_factor_secret_enc:
        session['user_id'] = user.id
        session['user_name'] = user.name
        ensure_user_workspace(user.id)
        session.pop('pending_2fa_user_id', None)
        session.pop('pending_2fa_user_name', None)
        session.pop('pending_2fa_next', None)
        flash('Login successful.', 'success')
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        try:
            secret = decrypt_secret(security.two_factor_secret_enc)
        except Exception:
            flash('2FA secret could not be verified. Disable and re-enable 2FA.', 'danger')
            return render_template('login_2fa.html', pending_user_name=session.get('pending_2fa_user_name', ''))

        if verify_totp_code(secret, code):
            session['user_id'] = user.id
            session['user_name'] = user.name
            next_path = session.get('pending_2fa_next', '')
            session.pop('pending_2fa_user_id', None)
            session.pop('pending_2fa_user_name', None)
            session.pop('pending_2fa_next', None)
            flash('2FA verification successful.', 'success')
            log_audit('auth.login_2fa', f'user={user.email}')
            if isinstance(next_path, str) and next_path.startswith('/'):
                return redirect(next_path)
            return redirect(url_for('dashboard'))

        flash('Invalid 2FA code.', 'danger')

    return render_template('login_2fa.html', pending_user_name=session.get('pending_2fa_user_name', ''))


@app.route('/logout')
@login_required
def logout():
    log_audit('auth.logout')
    session.pop('user_id', None)
    session.pop('user_name', None)
    session.pop('pending_2fa_user_id', None)
    session.pop('pending_2fa_user_name', None)
    session.pop('pending_2fa_next', None)
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


def current_user() -> Optional[User]:
    user_id = session.get('user_id')
    if not user_id:
        return None
    return User.query.get(user_id)


@app.route('/verify-email')
def verify_email():
    token = request.args.get('token', '').strip()
    if not token:
        flash('Invalid verification link.', 'warning')
        return redirect(url_for('login'))

    matched = consume_security_token(token, 'email_verify')
    if not matched:
        flash('Verification link is invalid or expired.', 'danger')
        return redirect(url_for('login'))

    security = ensure_user_security(matched.user_id)
    security.email_verified = True
    db.session.commit()
    flash('Email verified successfully.', 'success')
    log_audit('security.email_verified', f'user_id={matched.user_id}')
    return redirect(url_for('dashboard') if session.get('user_id') else url_for('login'))


@app.route('/password-reset/request', methods=['GET', 'POST'])
def request_password_reset():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        user = User.query.filter(func.lower(User.email) == email).first()
        if user:
            reset_token = create_security_token(user.id, 'password_reset', ttl_minutes=30)
            reset_link = url_for('password_reset', token=reset_token, _external=False)
            flash(f'Password reset link: {reset_link}', 'info')
            log_audit('security.password_reset_requested', f'user={user.email}')
        else:
            flash('If account exists, a reset link has been generated.', 'info')
        return redirect(url_for('login'))

    return render_template('password_reset_request.html')


@app.route('/password-reset/<token>', methods=['GET', 'POST'])
def password_reset(token: str):
    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm_password = request.form.get('confirm_password', '')
        if not password:
            flash('Password is required.', 'warning')
            return render_template('password_reset.html', token=token)
        if password != confirm_password:
            flash('Password and confirm password do not match.', 'warning')
            return render_template('password_reset.html', token=token)

        matched = consume_security_token(token, 'password_reset')
        if not matched:
            flash('Reset link is invalid or expired.', 'danger')
            return redirect(url_for('request_password_reset'))

        user = User.query.get(matched.user_id)
        if not user:
            flash('User not found.', 'danger')
            return redirect(url_for('request_password_reset'))

        user.password_hash = generate_password_hash(password)
        db.session.commit()
        log_audit('security.password_reset_completed', f'user={user.email}')
        flash('Password reset successful. Please login.', 'success')
        return redirect(url_for('login'))

    return render_template('password_reset.html', token=token)


@app.route('/security')
@login_required
def security_center():
    user = current_user()
    security = ensure_user_security(user.id) if user else None
    setup_secret = generate_totp_secret() if security and not security.two_factor_enabled else ''
    recent_logs = (
        AuditLog.query
        .filter(AuditLog.user_id == user.id)
        .order_by(AuditLog.created_at.desc())
        .limit(50)
        .all()
        if user else []
    )
    return render_template('security.html', security=security, recent_logs=recent_logs, setup_secret=setup_secret)


@app.route('/security/send-verification', methods=['POST'])
@login_required
def security_send_verification():
    user = current_user()
    if not user:
        return redirect(url_for('login'))
    security = ensure_user_security(user.id)
    if security.email_verified:
        flash('Email is already verified.', 'info')
        return redirect(url_for('security_center'))
    token = create_security_token(user.id, 'email_verify', ttl_minutes=60 * 24)
    verify_link = url_for('verify_email', token=token, _external=False)
    flash(f'Email verification link: {verify_link}', 'info')
    log_audit('security.email_verify_link_generated')
    return redirect(url_for('security_center'))


@app.route('/security/2fa/setup', methods=['POST'])
@login_required
def setup_two_factor():
    user = current_user()
    if not user:
        return redirect(url_for('login'))
    security = ensure_user_security(user.id)

    code = request.form.get('code', '').strip()
    secret = request.form.get('secret', '').strip()
    if not secret:
        flash('Missing 2FA secret. Please retry setup.', 'warning')
        return redirect(url_for('security_center'))

    if not verify_totp_code(secret, code):
        flash('Invalid code. 2FA not enabled.', 'danger')
        return redirect(url_for('security_center'))

    security.two_factor_secret_enc = encrypt_secret(secret)
    security.two_factor_enabled = True
    db.session.commit()
    log_audit('security.2fa_enabled')
    flash('2FA enabled successfully.', 'success')
    return redirect(url_for('security_center'))


@app.route('/security/2fa/disable', methods=['POST'])
@login_required
def disable_two_factor():
    user = current_user()
    if not user:
        return redirect(url_for('login'))
    security = ensure_user_security(user.id)
    security.two_factor_enabled = False
    security.two_factor_secret_enc = None
    db.session.commit()
    log_audit('security.2fa_disabled')
    flash('2FA disabled.', 'info')
    return redirect(url_for('security_center'))


def build_backup_payload(user_id: int) -> dict:
    return {
        'exported_at': datetime.utcnow().isoformat(),
        'accounts': [
            {'name': a.name, 'account_type': a.account_type, 'opening_balance': float(a.opening_balance), 'currency': a.currency}
            for a in accounts_for_user_query(user_id).order_by(Account.id).all()
        ],
        'categories': [
            {'name': c.name, 'kind': c.kind, 'icon': c.icon, 'color': c.color, 'is_active': c.is_active}
            for c in Category.query.order_by(Category.id).all()
        ],
        'transactions': [
            {
                'tx_date': t.tx_date.isoformat(),
                'description': t.description,
                'amount': float(t.amount),
                'tx_type': t.tx_type,
                'notes': t.notes,
                'payee': t.payee,
                'reference_no': t.reference_no,
                'source': t.source,
                'account_name': t.account.name,
                'category_name': t.category.name,
            }
            for t in transactions_for_user_query(user_id).order_by(Transaction.id).all()
        ],
        'investment_assets': [
            {
                'symbol': a.symbol,
                'name': a.name,
                'asset_class': a.asset_class,
                'currency': a.currency,
                'last_price': float(a.last_price),
                'last_price_at': a.last_price_at.isoformat() if a.last_price_at else None,
                'is_active': a.is_active,
            }
            for a in InvestmentAsset.query.order_by(InvestmentAsset.id).all()
        ],
        'investment_transactions': [
            {
                'symbol': tx.asset.symbol if tx.asset else None,
                'tx_date': tx.tx_date.isoformat(),
                'tx_kind': tx.tx_kind,
                'quantity': float(tx.quantity),
                'unit_price': float(tx.unit_price),
                'fees': float(tx.fees),
                'notes': tx.notes,
                'source': tx.source,
            }
            for tx in InvestmentTransaction.query.order_by(InvestmentTransaction.id).all()
        ],
        'recurring_rules': [
            {
                'name': r.name,
                'description': r.description,
                'amount': float(r.amount),
                'tx_type': r.tx_type,
                'payee': r.payee,
                'notes': r.notes,
                'frequency': r.frequency,
                'interval_value': r.interval_value,
                'next_run_date': r.next_run_date.isoformat(),
                'is_active': r.is_active,
                'account_name': Account.query.get(r.account_id).name if Account.query.get(r.account_id) else None,
                'category_name': Category.query.get(r.category_id).name if Category.query.get(r.category_id) else None,
            }
            for r in (
                RecurringTransaction.query
                .join(Account, RecurringTransaction.account_id == Account.id)
                .filter(Account.user_id == user_id)
                .order_by(RecurringTransaction.id)
                .all()
            )
        ],
        'bills': [
            {
                'name': b.name,
                'bill_kind': b.bill_kind,
                'amount_due': float(b.amount_due),
                'minimum_due': float(b.minimum_due),
                'annual_interest_rate': float(b.annual_interest_rate),
                'outstanding_balance': float(b.outstanding_balance),
                'next_due_date': b.next_due_date.isoformat(),
                'notes': b.notes,
                'is_active': b.is_active,
                'account_name': Account.query.get(b.account_id).name if b.account_id and Account.query.get(b.account_id) else None,
            }
            for b in (
                Bill.query
                .join(Account, Bill.account_id == Account.id)
                .filter(Account.user_id == user_id)
                .order_by(Bill.id)
                .all()
            )
        ],
        'budgets': [
            {'month_key': b.month_key, 'category_name': b.category.name, 'amount_limit': float(b.amount_limit)}
            for b in MonthlyBudget.query.filter_by(user_id=user_id).order_by(MonthlyBudget.id).all()
        ],
        'savings_goals': [
            {
                'name': g.name,
                'target_amount': float(g.target_amount),
                'current_saved': float(g.current_saved),
                'target_date': g.target_date.isoformat() if g.target_date else None,
                'is_completed': g.is_completed,
            }
            for g in SavingsGoal.query.filter_by(user_id=user_id).order_by(SavingsGoal.id).all()
        ],
    }


@app.route('/security/backup/download')
@login_required
def download_backup():
    user_id = current_user_id()
    payload = build_backup_payload(user_id) if user_id else {'exported_at': datetime.utcnow().isoformat()}
    output = BytesIO(json.dumps(payload, indent=2).encode('utf-8'))
    output.seek(0)
    filename = f"money_manager_backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    log_audit('security.backup_download')
    return send_file(output, as_attachment=True, download_name=filename, mimetype='application/json')


@app.route('/security/backup/restore', methods=['POST'])
@login_required
def restore_backup():
    user_id = current_user_id()
    file = request.files.get('backup_file')
    if not file or not file.filename:
        flash('Please choose a backup JSON file.', 'warning')
        return redirect(url_for('security_center'))
    try:
        payload = json.load(file)
    except Exception:
        flash('Invalid backup file format.', 'danger')
        return redirect(url_for('security_center'))

    try:
        account_map = {a.name: a for a in accounts_for_user_query(user_id).all()}
        for item in payload.get('accounts', []):
            if item.get('name') in account_map:
                continue
            account = Account(
                name=next_available_account_name(item.get('name', '').strip(), user_id),
                account_type=item.get('account_type') or 'Cash',
                opening_balance=Decimal(str(item.get('opening_balance', 0))),
                currency=item.get('currency') or 'INR',
                user_id=user_id,
            )
            db.session.add(account)
            db.session.flush()
            account_map[account.name] = account

        category_map = {c.name: c for c in Category.query.all()}
        for item in payload.get('categories', []):
            if item.get('name') in category_map:
                continue
            mapped_icon, mapped_color = CATEGORY_STYLE_MAP.get(
                str(item.get('name', '')).strip().lower(),
                ('bi bi-tag', '#6c757d'),
            )
            icon_value = item.get('icon') or 'bi bi-tag'
            if icon_value.strip() == 'bi bi-tag':
                icon_value = mapped_icon
            color_value = item.get('color') or mapped_color
            if is_placeholder_color(color_value):
                color_value = mapped_color
            category = Category(
                name=item.get('name', '').strip(),
                kind=item.get('kind') or 'expense',
                icon=icon_value,
                color=color_value,
                is_active=bool(item.get('is_active', True)),
            )
            db.session.add(category)
            db.session.flush()
            category_map[category.name] = category

        restored_tx = 0
        for item in payload.get('transactions', []):
            account = account_map.get(item.get('account_name', ''))
            category = category_map.get(item.get('category_name', ''))
            if not account or not category:
                continue
            description = str(item.get('description', '')).strip()
            tx_date_val = pd.to_datetime(item.get('tx_date'), errors='coerce')
            if not description or pd.isna(tx_date_val):
                continue
            exists = Transaction.query.filter_by(
                tx_date=tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val,
                description=description,
                amount=Decimal(str(item.get('amount', 0))),
                account_id=account.id,
                category_id=category.id,
                user_id=user_id,
            ).first()
            if exists:
                continue

            tx = Transaction(
                tx_date=tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val,
                description=description,
                amount=Decimal(str(item.get('amount', 0))),
                tx_type=item.get('tx_type') or 'expense',
                notes=item.get('notes'),
                payee=item.get('payee'),
                reference_no=item.get('reference_no'),
                source=item.get('source') or 'restore',
                account_id=account.id,
                category_id=category.id,
                user_id=user_id,
            )
            db.session.add(tx)
            restored_tx += 1

        asset_map = {str(a.symbol or '').upper(): a for a in InvestmentAsset.query.all()}
        restored_assets = 0
        for item in payload.get('investment_assets', []):
            symbol = str(item.get('symbol', '')).strip().upper()
            name = str(item.get('name', '')).strip()
            if not symbol or not name:
                continue

            asset = asset_map.get(symbol)
            normalized_class = normalize_asset_class(item.get('asset_class', 'stock'))
            currency = str(item.get('currency', 'INR')).strip().upper() or 'INR'
            price = parse_decimal_or_none(item.get('last_price', 0)) or Decimal('0')
            price_date_raw = item.get('last_price_at')
            price_date = pd.to_datetime(price_date_raw, errors='coerce') if price_date_raw else pd.NaT

            if not asset:
                asset = InvestmentAsset(
                    symbol=symbol,
                    name=name,
                    asset_class=normalized_class,
                    currency=currency,
                    last_price=price,
                    last_price_at=(price_date.date() if not pd.isna(price_date) else None),
                    is_active=bool(item.get('is_active', True)),
                )
                db.session.add(asset)
                db.session.flush()
                asset_map[symbol] = asset
                restored_assets += 1
            else:
                asset.name = name or asset.name
                asset.asset_class = normalized_class
                asset.currency = currency
                asset.is_active = bool(item.get('is_active', asset.is_active))
                if price > 0:
                    asset.last_price = price
                if not pd.isna(price_date):
                    asset.last_price_at = price_date.date()

        restored_inv_tx = 0
        for item in payload.get('investment_transactions', []):
            symbol = str(item.get('symbol', '')).strip().upper()
            if not symbol:
                continue
            asset = asset_map.get(symbol)
            if not asset:
                asset = InvestmentAsset(
                    symbol=symbol,
                    name=symbol,
                    asset_class='other',
                    currency='INR',
                    last_price=0,
                    is_active=True,
                )
                db.session.add(asset)
                db.session.flush()
                asset_map[symbol] = asset
                restored_assets += 1

            tx_date_val = pd.to_datetime(item.get('tx_date'), errors='coerce')
            if pd.isna(tx_date_val):
                continue
            tx_date_obj = tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val

            tx_kind = normalize_investment_kind(item.get('tx_kind', 'buy'))
            quantity = parse_decimal_or_none(item.get('quantity', 0)) or Decimal('0')
            unit_price = parse_decimal_or_none(item.get('unit_price', 0)) or Decimal('0')
            fees = parse_decimal_or_none(item.get('fees', 0)) or Decimal('0')

            if tx_kind in {'buy', 'sell', 'sip'} and quantity <= 0:
                continue
            if tx_kind == 'dividend' and quantity <= 0 and unit_price <= 0:
                continue

            exists = InvestmentTransaction.query.filter_by(
                asset_id=asset.id,
                tx_date=tx_date_obj,
                tx_kind=tx_kind,
                quantity=quantity,
                unit_price=unit_price,
                fees=fees,
            ).first()
            if exists:
                continue

            tx = InvestmentTransaction(
                asset_id=asset.id,
                tx_date=tx_date_obj,
                tx_kind=tx_kind,
                quantity=quantity,
                unit_price=unit_price,
                fees=fees,
                notes=item.get('notes'),
                source=item.get('source') or 'restore',
            )
            db.session.add(tx)
            restored_inv_tx += 1

        db.session.commit()
        log_audit(
            'security.backup_restore',
            f'transactions={restored_tx},investment_assets={restored_assets},investment_transactions={restored_inv_tx}',
        )
        flash(
            f'Backup restored. Added {restored_tx} records, {restored_assets} assets, {restored_inv_tx} investment transactions.',
            'success',
        )
    except Exception as exc:
        db.session.rollback()
        flash(f'Backup restore failed: {exc}', 'danger')
    return redirect(url_for('security_center'))


@app.route('/')
def home():
    if session.get('user_id'):
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/manifest.webmanifest')
def manifest():
    return send_from_directory(
        app.static_folder,
        'manifest.webmanifest',
        mimetype='application/manifest+json',
    )


@app.route('/sw.js')
def service_worker():
    response = send_from_directory(
        app.static_folder,
        'sw.js',
        mimetype='application/javascript',
    )
    response.headers['Cache-Control'] = 'no-cache'
    return response


@app.route('/dashboard')
@login_required
def dashboard():
    def parse_iso_date(raw_value: object, default: Optional[date] = None) -> Optional[date]:
        text = str(raw_value or '').strip()
        if not text:
            return default
        try:
            return date.fromisoformat(text)
        except Exception:
            return default

    def shift_anchor(period_name: str, anchor_date: date, step: int) -> date:
        if period_name == 'today':
            return anchor_date + timedelta(days=step)
        if period_name == 'week':
            return anchor_date + timedelta(days=step * 7)
        if period_name == 'month':
            return add_months(anchor_date, step)
        if period_name == 'year':
            target_year = anchor_date.year + step
            safe_day = min(anchor_date.day, calendar.monthrange(target_year, anchor_date.month)[1])
            return date(target_year, anchor_date.month, safe_day)
        return anchor_date

    def resolve_period_config() -> dict:
        today_local = datetime.utcnow().date()
        allowed_periods = {
            'today',
            'week',
            'month',
            'year',
            'rolling7',
            'rolling30',
            'rolling90',
            'rolling365',
            'custom',
            'all',
        }
        period = str(request.args.get('period', 'month') or 'month').strip().lower()
        if period not in allowed_periods:
            period = 'month'

        anchor_date = parse_iso_date(request.args.get('anchor', ''), today_local) or today_local
        start_date: Optional[date] = None
        end_date: Optional[date] = None
        can_navigate = period in {'today', 'week', 'month', 'year'}

        if period == 'today':
            start_date = anchor_date
            end_date = anchor_date
            label = anchor_date.strftime('%d %b %Y')
        elif period == 'week':
            start_date = anchor_date - timedelta(days=anchor_date.weekday())
            end_date = start_date + timedelta(days=6)
            label = f'{start_date.strftime("%d %b")} - {end_date.strftime("%d %b %Y")}'
        elif period == 'month':
            start_date = anchor_date.replace(day=1)
            end_date = add_months(start_date, 1) - timedelta(days=1)
            label = anchor_date.strftime('%B %Y')
        elif period == 'year':
            start_date = date(anchor_date.year, 1, 1)
            end_date = date(anchor_date.year, 12, 31)
            label = str(anchor_date.year)
        elif period.startswith('rolling'):
            rolling_days = int(period.replace('rolling', '') or '30')
            end_date = anchor_date
            start_date = end_date - timedelta(days=max(1, rolling_days) - 1)
            label = f'Last {rolling_days} days'
        elif period == 'custom':
            start_date = parse_iso_date(request.args.get('start', ''), None)
            end_date = parse_iso_date(request.args.get('end', ''), None)
            if not start_date and not end_date:
                end_date = anchor_date
                start_date = end_date - timedelta(days=29)
            elif not start_date:
                start_date = end_date
            elif not end_date:
                end_date = start_date
            if start_date and end_date and start_date > end_date:
                start_date, end_date = end_date, start_date
            anchor_date = end_date or anchor_date
            label = f'{start_date.strftime("%d %b %Y")} - {end_date.strftime("%d %b %Y")}'
        else:
            period = 'all'
            label = 'All time'

        return {
            'period': period,
            'anchor_date': anchor_date,
            'start_date': start_date,
            'end_date': end_date,
            'label': label,
            'can_navigate': can_navigate,
            'today': today_local,
        }

    user_id = current_user_id()
    accounts = accounts_for_user_query(user_id).order_by(Account.name).all()
    period_cfg = resolve_period_config()
    today = period_cfg['today']
    start_date = period_cfg['start_date']
    end_date = period_cfg['end_date']

    period_query = transactions_for_user_query(user_id)
    if start_date and end_date:
        range_start_dt = datetime.combine(start_date, datetime.min.time())
        range_end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        period_query = period_query.filter(Transaction.tx_date >= range_start_dt, Transaction.tx_date < range_end_dt)

    period_transactions = period_query.order_by(Transaction.tx_date.desc()).all()
    transactions = period_transactions[:8]
    period_days = 30
    if start_date and end_date:
        period_days = max(1, (end_date - start_date).days + 1)
    elif period_transactions:
        unique_days = {tx.tx_date.date() for tx in period_transactions}
        period_days = max(1, len(unique_days))

    income_total = sum([Decimal(tx.amount) for tx in period_transactions if tx.tx_type == 'income'], Decimal('0'))
    expense_total = sum([Decimal(tx.amount) for tx in period_transactions if tx.tx_type == 'expense'], Decimal('0'))
    cash_flow = income_total - expense_total
    tx_count = len(period_transactions)
    expense_transactions = [tx for tx in period_transactions if tx.tx_type == 'expense']
    avg_expense_tx = (
        (sum([Decimal(tx.amount) for tx in expense_transactions], Decimal('0')) / Decimal(len(expense_transactions)))
        if expense_transactions else Decimal('0')
    )
    largest_expense_tx = max(expense_transactions, key=lambda tx: Decimal(tx.amount), default=None)
    income_expense_ratio = 0.0
    if expense_total > 0:
        income_expense_ratio = float((income_total / expense_total))

    opening_total = accounts_for_user_query(user_id).with_entities(func.coalesce(func.sum(Account.opening_balance), 0)).scalar() or 0
    all_income_total = transactions_for_user_query(user_id).with_entities(func.coalesce(func.sum(Transaction.amount), 0)).filter(Transaction.tx_type == 'income').scalar() or 0
    all_expense_total = transactions_for_user_query(user_id).with_entities(func.coalesce(func.sum(Transaction.amount), 0)).filter(Transaction.tx_type == 'expense').scalar() or 0
    balance = Decimal(opening_total) + Decimal(all_income_total) - Decimal(all_expense_total)
    burn_rate_daily = (expense_total / Decimal(period_days)) if period_days > 0 else Decimal('0')
    runway_days = Decimal('0')
    if burn_rate_daily > 0 and balance > 0:
        runway_days = (balance / burn_rate_daily).quantize(Decimal('0.1'))

    use_daily_chart = bool(start_date and end_date and (end_date - start_date).days <= 62)
    trend_map: dict[str, dict[str, Decimal]] = {}
    for tx in sorted(period_transactions, key=lambda item: item.tx_date):
        key = tx.tx_date.strftime('%Y-%m-%d' if use_daily_chart else '%Y-%m')
        if key not in trend_map:
            trend_map[key] = {'income': Decimal('0'), 'expense': Decimal('0')}
        if tx.tx_type == 'income':
            trend_map[key]['income'] += Decimal(tx.amount)
        elif tx.tx_type == 'expense':
            trend_map[key]['expense'] += Decimal(tx.amount)

    chart_labels = list(trend_map.keys())
    income_series = [float(trend_map[label]['income']) for label in chart_labels]
    expense_series = [float(trend_map[label]['expense']) for label in chart_labels]
    current_month_income = income_total
    current_month_expense = expense_total
    savings_rate = 0.0
    if Decimal(current_month_income) > 0:
        savings_rate = float(((Decimal(current_month_income) - Decimal(current_month_expense)) / Decimal(current_month_income)) * Decimal('100'))

    category_totals: dict[int, Decimal] = {}
    for tx in period_transactions:
        if tx.tx_type != 'expense':
            continue
        category_totals[tx.category_id] = category_totals.get(tx.category_id, Decimal('0')) + Decimal(tx.amount)

    categories_by_id = {category.id: category for category in Category.query.all()}
    sorted_category_totals = sorted(category_totals.items(), key=lambda item: item[1], reverse=True)

    top_month_categories = []
    for category_id, total in sorted_category_totals[:5]:
        category = categories_by_id.get(category_id)
        top_month_categories.append(
            {
                'category': category.name if category else 'Other',
                'total': total,
            }
        )
    highest_expense_category = top_month_categories[0]['category'] if top_month_categories else 'N/A'
    top_expense_share_pct = 0.0
    if expense_total > 0 and sorted_category_totals:
        top_expense_share_pct = float((sorted_category_totals[0][1] / expense_total) * Decimal('100'))

    daily_tx_counts: dict[date, int] = {}
    for tx in period_transactions:
        day_key = tx.tx_date.date()
        daily_tx_counts[day_key] = daily_tx_counts.get(day_key, 0) + 1

    busiest_day_label = 'N/A'
    busiest_day_count = 0
    if daily_tx_counts:
        busiest_day, busiest_day_count = max(daily_tx_counts.items(), key=lambda item: (item[1], item[0]))
        busiest_day_label = busiest_day.strftime('%d %b %Y')

    avg_daily_tx = (Decimal(tx_count) / Decimal(period_days)) if period_days > 0 else Decimal('0')
    avg_daily_net = (cash_flow / Decimal(period_days)) if period_days > 0 else Decimal('0')

    command_center_notes: list[str] = []
    if cash_flow < 0:
        command_center_notes.append('Spending is higher than income for this range.')
    elif cash_flow > 0:
        command_center_notes.append('Positive net cashflow in this range.')
    else:
        command_center_notes.append('Cashflow is neutral in this range.')
    if top_expense_share_pct >= 40:
        command_center_notes.append(
            f'{highest_expense_category} contributes {top_expense_share_pct:.1f}% of expenses.'
        )
    if runway_days > 0 and runway_days < Decimal('30'):
        command_center_notes.append('Runway is below 30 days at the current burn rate.')
    if not command_center_notes:
        command_center_notes.append('Add more records to unlock stronger insights.')

    upcoming_recurring = (
        RecurringTransaction.query
        .join(Account, RecurringTransaction.account_id == Account.id)
        .filter(RecurringTransaction.is_active.is_(True))
        .filter(Account.user_id == user_id)
        .filter(RecurringTransaction.next_run_date >= today, RecurringTransaction.next_run_date <= today + timedelta(days=7))
        .order_by(RecurringTransaction.next_run_date.asc())
        .limit(6)
        .all()
    )
    upcoming_bills = (
        Bill.query
        .join(Account, Bill.account_id == Account.id)
        .filter(Bill.is_active.is_(True))
        .filter(Account.user_id == user_id)
        .filter(Bill.next_due_date >= today, Bill.next_due_date <= today + timedelta(days=10))
        .order_by(Bill.next_due_date.asc())
        .limit(6)
        .all()
    )
    overdue_bills = (
        Bill.query
        .join(Account, Bill.account_id == Account.id)
        .filter(Bill.is_active.is_(True))
        .filter(Account.user_id == user_id)
        .filter(Bill.next_due_date < today)
        .order_by(Bill.next_due_date.asc())
        .all()
    )
    expense_by_category_rows = []
    for category_id, total in sorted_category_totals:
        category = categories_by_id.get(category_id)
        expense_by_category_rows.append(
            {
                'category': category.name if category else 'Other',
                'icon': (category.icon if category and category.icon else 'bi bi-tag'),
                'color': (category.color if category and category.color else '#6c757d'),
                'total': total,
            }
        )

    expense_pie_palette = [
        '#22c55e', '#0ea5e9', '#f97316', '#a855f7', '#ef4444',
        '#14b8a6', '#eab308', '#3b82f6', '#f43f5e', '#84cc16',
    ]
    muted_colors = {'#6c757d', '#808080', '#7a7a7a', 'gray', 'grey'}

    expense_pie_labels = [row['category'] for row in expense_by_category_rows]
    expense_pie_icons = [row.get('icon') or 'bi bi-tag' for row in expense_by_category_rows]
    expense_pie_series = [float(row.get('total') or 0) for row in expense_by_category_rows]
    expense_pie_colors = []
    for idx, row in enumerate(expense_by_category_rows):
        raw_color = str(row.get('color') or '').strip().lower()
        if not raw_color or raw_color in muted_colors:
            expense_pie_colors.append(expense_pie_palette[idx % len(expense_pie_palette)])
        else:
            expense_pie_colors.append(row.get('color'))

    account_cards = []
    for account in accounts:
        income = transactions_for_user_query(user_id).with_entities(func.coalesce(func.sum(Transaction.amount), 0)).filter(
            Transaction.account_id == account.id,
            Transaction.tx_type == 'income',
        ).scalar() or 0
        expense = transactions_for_user_query(user_id).with_entities(func.coalesce(func.sum(Transaction.amount), 0)).filter(
            Transaction.account_id == account.id,
            Transaction.tx_type == 'expense',
        ).scalar() or 0
        current_balance = Decimal(account.opening_balance) + Decimal(income) - Decimal(expense)
        account_cards.append({'account': account, 'balance': current_balance})

    net_series = [income_value - expense_value for income_value, expense_value in zip(income_series, expense_series)]
    kpi_spark_burn = expense_series[-16:] if expense_series else [0.0]
    kpi_spark_runway = net_series[-16:] if net_series else [0.0]
    kpi_spark_accounts = [float(item['balance']) for item in account_cards][-16:] if account_cards else [0.0]
    kpi_spark_driver = [float(total) for _, total in sorted_category_totals[:8]] or [0.0]

    prev_period_url = None
    next_period_url = None
    if period_cfg['can_navigate']:
        prev_anchor = shift_anchor(period_cfg['period'], period_cfg['anchor_date'], -1)
        next_anchor = shift_anchor(period_cfg['period'], period_cfg['anchor_date'], 1)
        prev_period_url = url_for('dashboard', period=period_cfg['period'], anchor=prev_anchor.isoformat())
        next_period_url = url_for('dashboard', period=period_cfg['period'], anchor=next_anchor.isoformat())

    return render_template(
        'dashboard.html',
        accounts=account_cards,
        transactions=transactions,
        balance=float(balance),
        income_total=float(income_total),
        expense_total=float(expense_total),
        cash_flow=float(cash_flow),
        current_month_income=float(current_month_income),
        current_month_expense=float(current_month_expense),
        savings_rate=savings_rate,
        top_month_categories=top_month_categories,
        upcoming_recurring=upcoming_recurring,
        upcoming_bills=upcoming_bills,
        overdue_bills=overdue_bills,
        chart_labels=chart_labels,
        income_series=income_series,
        expense_series=expense_series,
        expense_pie_labels=expense_pie_labels,
        expense_pie_icons=expense_pie_icons,
        expense_pie_series=expense_pie_series,
        expense_pie_colors=expense_pie_colors,
        period_days=period_days,
        burn_rate_daily=float(burn_rate_daily),
        runway_days=float(runway_days),
        accounts_count=len(account_cards),
        highest_expense_category=highest_expense_category,
        kpi_spark_burn=kpi_spark_burn,
        kpi_spark_runway=kpi_spark_runway,
        kpi_spark_accounts=kpi_spark_accounts,
        kpi_spark_driver=kpi_spark_driver,
        top_expense_share_pct=top_expense_share_pct,
        tx_count=tx_count,
        avg_daily_tx=float(avg_daily_tx),
        avg_daily_net=float(avg_daily_net),
        busiest_day_label=busiest_day_label,
        busiest_day_count=busiest_day_count,
        command_center_notes=command_center_notes,
        avg_expense_tx=float(avg_expense_tx),
        largest_expense_desc=(largest_expense_tx.description if largest_expense_tx else 'N/A'),
        largest_expense_amount=float(Decimal(largest_expense_tx.amount) if largest_expense_tx else Decimal('0')),
        income_expense_ratio=income_expense_ratio,
        period_label=period_cfg['label'],
        selected_period=period_cfg['period'],
        selected_anchor=period_cfg['anchor_date'].isoformat(),
        selected_start=(period_cfg['start_date'].isoformat() if period_cfg['start_date'] else ''),
        selected_end=(period_cfg['end_date'].isoformat() if period_cfg['end_date'] else ''),
        prev_period_url=prev_period_url,
        next_period_url=next_period_url,
        today_iso=today.isoformat(),
    )


@app.route('/records')
@login_required
def records():
    user_id = current_user_id()
    filters = get_records_filters_from_request()
    query = apply_records_filters(
        transactions_for_user_query(user_id).join(Account, Transaction.account_id == Account.id).join(Category),
        filters,
    )
    sort, sort_order = resolve_records_sort(filters['sort'])
    filters['sort'] = sort

    transactions = query.order_by(sort_order, Transaction.id.desc()).all()
    tx_ids = [tx.id for tx in transactions]
    metadata_map: dict[int, TransactionMeta] = {}
    if tx_ids:
        meta_rows = TransactionMeta.query.filter(TransactionMeta.transaction_id.in_(tx_ids)).all()
        metadata_map = {row.transaction_id: row for row in meta_rows}
    total = sum([Decimal(t.amount) if t.tx_type == 'income' else -Decimal(t.amount) for t in transactions], Decimal('0'))
    currencies = [
        row[0] for row in db.session.query(Account.currency)
        .filter(Account.user_id == user_id, Account.currency.isnot(None), Account.currency != '')
        .distinct()
        .order_by(Account.currency)
        .all()
    ]
    payment_type_options = ['cash', 'upi', 'card', 'bank_transfer', 'wallet', 'cheque', 'other']

    return render_template(
        'records.html',
        transactions=transactions,
        accounts=accounts_for_user_query(user_id).order_by(Account.name).all(),
        categories=Category.query.order_by(Category.name).all(),
        currencies=currencies,
        filters=filters,
        metadata_map=metadata_map,
        payment_type_options=payment_type_options,
        total=float(total),
    )


@app.route('/records/export')
@login_required
def export_records():
    user_id = current_user_id()
    filters = get_records_filters_from_request()
    query = apply_records_filters(
        transactions_for_user_query(user_id).join(Account, Transaction.account_id == Account.id).join(Category),
        filters,
    )
    _, sort_order = resolve_records_sort(filters['sort'])
    transactions = query.order_by(sort_order, Transaction.id.desc()).all()

    rows = build_records_export_rows(transactions)

    dataframe = pd.DataFrame(
        rows,
        columns=[
            'Date', 'Description', 'Account', 'Category', 'Type', 'Amount',
            'Notes', 'Payee', 'Label', 'Payment Type', 'Reference No', 'Source',
        ],
    )
    output = BytesIO()
    dataframe.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    filename = f"records_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


@app.route('/records/bulk/export', methods=['POST'])
@login_required
def bulk_export_records():
    user_id = current_user_id()
    selected_ids = parse_selected_transaction_ids_from_request()
    if not selected_ids:
        flash('Please select at least one record to export.', 'warning')
        return redirect(url_for('records'))

    transactions = (
        transactions_for_user_query(user_id)
        .join(Account, Transaction.account_id == Account.id)
        .join(Category)
        .filter(Transaction.id.in_(selected_ids))
        .order_by(Transaction.tx_date.desc(), Transaction.id.desc())
        .all()
    )
    if not transactions:
        flash('No valid records found for selected export.', 'warning')
        return redirect(url_for('records'))

    rows = build_records_export_rows(transactions)
    dataframe = pd.DataFrame(
        rows,
        columns=[
            'Date', 'Description', 'Account', 'Category', 'Type', 'Amount',
            'Notes', 'Payee', 'Label', 'Payment Type', 'Reference No', 'Source',
        ],
    )
    output = BytesIO()
    dataframe.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    filename = f"records_bulk_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    log_audit('records.bulk_export', f'count={len(transactions)}')
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


@app.route('/records/bulk/delete', methods=['POST'])
@login_required
def bulk_delete_records():
    user_id = current_user_id()
    selected_ids = parse_selected_transaction_ids_from_request()
    if not selected_ids:
        flash('Please select at least one record to delete.', 'warning')
        return redirect(url_for('records'))

    try:
        allowed_ids = [
            row[0]
            for row in transactions_for_user_query(user_id)
            .with_entities(Transaction.id)
            .filter(Transaction.id.in_(selected_ids))
            .all()
        ]
        if not allowed_ids:
            flash('No valid records found for selected delete.', 'warning')
            return redirect(url_for('records'))
        TransactionMeta.query.filter(TransactionMeta.transaction_id.in_(allowed_ids)).delete(synchronize_session=False)
        deleted_count = Transaction.query.filter(Transaction.id.in_(allowed_ids)).delete(synchronize_session=False)
        db.session.commit()
        log_audit('records.bulk_delete', f'count={deleted_count}')
        flash(f'Deleted {deleted_count} selected record(s).', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not delete selected records: {exc}', 'danger')
    return redirect(url_for('records'))


@app.route('/records/bulk/edit', methods=['POST'])
@login_required
def bulk_edit_records():
    user_id = current_user_id()
    selected_ids = parse_selected_transaction_ids_from_request()
    if not selected_ids:
        flash('Please select at least one record to edit.', 'warning')
        return redirect(url_for('records'))

    category_id = request.form.get('category_id', type=int)
    payee = str(request.form.get('payee', '') or '').strip()
    notes = str(request.form.get('notes', '') or '').strip()
    label = str(request.form.get('label', '') or '').strip()
    payment_type = str(request.form.get('payment_type', '') or '').strip().lower()
    valid_payment_types = {'cash', 'upi', 'card', 'bank_transfer', 'wallet', 'cheque', 'other'}
    if payment_type and payment_type not in valid_payment_types:
        payment_type = ''

    category = None
    if category_id:
        category = Category.query.get(category_id)
        if not category:
            flash('Selected category is invalid.', 'warning')
            return redirect(url_for('records'))

    transactions = transactions_for_user_query(user_id).filter(Transaction.id.in_(selected_ids)).all()
    if not transactions:
        flash('No valid records found for selected edit.', 'warning')
        return redirect(url_for('records'))

    try:
        updated_count = 0
        for tx in transactions:
            if category:
                tx.category_id = category.id
            if payee:
                tx.payee = payee
            if notes:
                tx.notes = notes

            if label or payment_type:
                meta = tx.tx_meta
                if not meta:
                    meta = TransactionMeta(transaction_id=tx.id)
                    db.session.add(meta)
                if label:
                    meta.label = label
                if payment_type:
                    meta.payment_type = payment_type
            updated_count += 1

        db.session.commit()
        log_audit('records.bulk_edit', f'count={updated_count}')
        flash(f'Updated {updated_count} selected record(s).', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not update selected records: {exc}', 'danger')

    return redirect(url_for('records'))


@app.route('/recurring', methods=['GET', 'POST'])
@login_required
def recurring():
    user_id = current_user_id()
    accounts = accounts_for_user_query(user_id).order_by(Account.name).all()
    categories = Category.query.filter_by(is_active=True).order_by(Category.kind, Category.name).all()

    if request.method == 'POST':
        try:
            next_run_date = datetime.strptime(request.form['next_run_date'], '%Y-%m-%d').date()
            interval_value = max(1, int(request.form.get('interval_value', '1') or '1'))
            account = accounts_for_user_query(user_id).filter(Account.id == int(request.form['account_id'])).first()
            if not account:
                flash('Please choose a valid account for your profile.', 'warning')
                return redirect(url_for('recurring'))
            item = RecurringTransaction(
                name=request.form['name'].strip(),
                description=request.form.get('description', '').strip() or request.form['name'].strip(),
                amount=Decimal(request.form['amount']),
                tx_type=request.form['tx_type'],
                payee=request.form.get('payee', '').strip() or None,
                notes=request.form.get('notes', '').strip() or None,
                frequency=request.form['frequency'],
                interval_value=interval_value,
                next_run_date=next_run_date,
                account_id=account.id,
                category_id=int(request.form['category_id']),
                is_active=True,
            )
            db.session.add(item)
            db.session.commit()
            session.pop('last_recurring_run', None)
            log_audit('recurring.rule_created', f'name={item.name}')
            flash('Recurring rule created.', 'success')
            return redirect(url_for('recurring'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not create recurring rule: {exc}', 'danger')

    rules = (
        RecurringTransaction.query
        .join(Account, RecurringTransaction.account_id == Account.id)
        .filter(Account.user_id == user_id)
        .order_by(RecurringTransaction.next_run_date.asc(), RecurringTransaction.name.asc())
        .all()
    )
    return render_template(
        'recurring.html',
        rules=rules,
        accounts=accounts,
        categories=categories,
    )


@app.route('/recurring/<int:rule_id>/toggle', methods=['POST'])
@login_required
def toggle_recurring(rule_id: int):
    user_id = current_user_id()
    rule = (
        RecurringTransaction.query
        .join(Account, RecurringTransaction.account_id == Account.id)
        .filter(Account.user_id == user_id, RecurringTransaction.id == rule_id)
        .first_or_404()
    )
    rule.is_active = not rule.is_active
    db.session.commit()
    log_audit('recurring.rule_toggled', f'rule_id={rule.id},active={rule.is_active}')
    flash('Recurring rule status updated.', 'success')
    return redirect(url_for('recurring'))


@app.route('/recurring/<int:rule_id>/delete', methods=['POST'])
@login_required
def delete_recurring(rule_id: int):
    user_id = current_user_id()
    rule = (
        RecurringTransaction.query
        .join(Account, RecurringTransaction.account_id == Account.id)
        .filter(Account.user_id == user_id, RecurringTransaction.id == rule_id)
        .first_or_404()
    )
    db.session.delete(rule)
    db.session.commit()
    log_audit('recurring.rule_deleted', f'rule_id={rule_id}')
    flash('Recurring rule deleted.', 'success')
    return redirect(url_for('recurring'))


@app.route('/bills', methods=['GET', 'POST'])
@login_required
def bills():
    user_id = current_user_id()
    accounts = accounts_for_user_query(user_id).order_by(Account.name).all()

    if request.method == 'POST':
        try:
            next_due_date = datetime.strptime(request.form['next_due_date'], '%Y-%m-%d').date()
            account_id = request.form.get('account_id', type=int)
            account = accounts_for_user_query(user_id).filter(Account.id == account_id).first() if account_id else None
            if not account:
                flash('Please choose a valid account for your profile.', 'warning')
                return redirect(url_for('bills'))
            bill = Bill(
                name=request.form['name'].strip(),
                bill_kind=request.form['bill_kind'],
                amount_due=Decimal(request.form['amount_due'] or '0'),
                minimum_due=Decimal(request.form.get('minimum_due', '0') or '0'),
                annual_interest_rate=Decimal(request.form.get('annual_interest_rate', '0') or '0'),
                outstanding_balance=Decimal(request.form.get('outstanding_balance', '0') or '0'),
                next_due_date=next_due_date,
                account_id=account.id,
                notes=request.form.get('notes', '').strip() or None,
                is_active=True,
            )
            db.session.add(bill)
            db.session.commit()
            log_audit('bills.item_created', f'name={bill.name}')
            flash('Bill/debt item added.', 'success')
            return redirect(url_for('bills'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not add bill: {exc}', 'danger')

    all_bills = (
        Bill.query
        .join(Account, Bill.account_id == Account.id)
        .filter(Account.user_id == user_id)
        .order_by(Bill.next_due_date.asc(), Bill.name.asc())
        .all()
    )
    planner = build_payment_planner(all_bills, datetime.utcnow().date())
    return render_template('bills.html', bills=all_bills, planner=planner, accounts=accounts, today=datetime.utcnow().date())


@app.route('/bills/<int:bill_id>/pay', methods=['POST'])
@login_required
def pay_bill(bill_id: int):
    user_id = current_user_id()
    bill = (
        Bill.query
        .join(Account, Bill.account_id == Account.id)
        .filter(Account.user_id == user_id, Bill.id == bill_id)
        .first_or_404()
    )
    amount_paid = parse_decimal_or_none(request.form.get('amount_paid', ''))
    if amount_paid is None or amount_paid <= 0:
        flash('Enter a valid payment amount.', 'warning')
        return redirect(url_for('bills'))

    try:
        interest_component = min(amount_paid, bill_interest_estimate(bill))
        principal_component = max(Decimal('0'), amount_paid - interest_component)
        payment = BillPayment(
            bill_id=bill.id,
            amount_paid=amount_paid,
            interest_component=interest_component,
            principal_component=principal_component,
            notes=request.form.get('notes', '').strip() or None,
        )
        db.session.add(payment)

        if Decimal(bill.outstanding_balance or 0) > 0:
            bill.outstanding_balance = max(Decimal('0'), Decimal(bill.outstanding_balance) - principal_component)
        bill.next_due_date = add_months(bill.next_due_date, 1)

        if request.form.get('record_payment') == 'on' and bill.account_id:
            category = infer_category('Bills', 'expense')
            transaction = Transaction(
                tx_date=datetime.utcnow(),
                description=f'Bill Payment - {bill.name}',
                amount=amount_paid,
                tx_type='expense',
                notes='Recorded from bill payment planner',
                payee=bill.name,
                source='bill',
                reference_no=f'bill-{bill.id}-{datetime.utcnow().date().isoformat()}',
                account_id=bill.account_id,
                category_id=category.id,
                user_id=user_id,
            )
            db.session.add(transaction)

        db.session.commit()
        log_audit('bills.payment_recorded', f'bill_id={bill.id},amount={amount_paid}')
        flash('Bill payment recorded.', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not record payment: {exc}', 'danger')
    return redirect(url_for('bills'))


@app.route('/bills/<int:bill_id>/toggle', methods=['POST'])
@login_required
def toggle_bill(bill_id: int):
    user_id = current_user_id()
    bill = (
        Bill.query
        .join(Account, Bill.account_id == Account.id)
        .filter(Account.user_id == user_id, Bill.id == bill_id)
        .first_or_404()
    )
    bill.is_active = not bill.is_active
    db.session.commit()
    log_audit('bills.item_toggled', f'bill_id={bill.id},active={bill.is_active}')
    flash('Bill status updated.', 'success')
    return redirect(url_for('bills'))


@app.route('/investments')
@login_required
def investments():
    snapshot = calculate_portfolio_snapshot()
    assets = InvestmentAsset.query.order_by(InvestmentAsset.symbol.asc()).all()
    focus_asset = request.args.get('focus_asset', '').strip().upper()
    recent_transactions = (
        InvestmentTransaction.query
        .order_by(InvestmentTransaction.tx_date.desc(), InvestmentTransaction.id.desc())
        .limit(20)
        .all()
    )
    return render_template(
        'investments.html',
        assets=assets,
        recent_transactions=recent_transactions,
        snapshot=snapshot,
        focus_asset=focus_asset,
        today_iso=datetime.utcnow().date().isoformat(),
        asset_class_choices=['stock', 'mutual_fund', 'etf', 'bond', 'other'],
        kind_choices=['buy', 'sip', 'sell', 'dividend'],
    )


@app.route('/investments/assets/add', methods=['POST'])
@login_required
def add_investment_asset():
    redirect_target = url_for('investments')
    try:
        symbol = request.form.get('symbol', '').strip().upper()
        name = request.form.get('name', '').strip()
        asset_class = normalize_asset_class(request.form.get('asset_class', 'stock'))
        currency = request.form.get('currency', 'INR').strip().upper() or 'INR'
        last_price = parse_decimal_or_none(request.form.get('last_price', '0')) or Decimal('0')

        if not symbol or not name:
            flash('Symbol and name are required.', 'warning')
            return redirect(url_for('investments'))

        asset = InvestmentAsset.query.filter(func.upper(InvestmentAsset.symbol) == symbol).first()
        if not asset:
            asset = InvestmentAsset(
                symbol=symbol,
                name=name,
                asset_class=asset_class,
                currency=currency,
                last_price=last_price,
                last_price_at=datetime.utcnow().date() if last_price > 0 else None,
                is_active=True,
            )
            db.session.add(asset)
        else:
            asset.name = name
            asset.asset_class = asset_class
            asset.currency = currency
            if last_price > 0:
                asset.last_price = last_price
                asset.last_price_at = datetime.utcnow().date()

        db.session.commit()
        log_audit('investments.asset_saved', f'symbol={symbol}')
        flash('Investment asset saved.', 'success')
        redirect_target = url_for('investments', focus_asset=symbol)
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not save asset: {exc}', 'danger')
    return redirect(redirect_target)


@app.route('/investments/assets/<int:asset_id>/toggle', methods=['POST'])
@login_required
def toggle_investment_asset(asset_id: int):
    asset = InvestmentAsset.query.get_or_404(asset_id)
    asset.is_active = not asset.is_active
    db.session.commit()
    log_audit('investments.asset_toggled', f'asset_id={asset.id},active={asset.is_active}')
    flash('Asset status updated.', 'success')
    return redirect(url_for('investments'))


@app.route('/investments/transactions/add', methods=['POST'])
@login_required
def add_investment_transaction():
    try:
        asset_id = request.form.get('asset_id', type=int)
        tx_date_raw = request.form.get('tx_date', '').strip()
        tx_kind = normalize_investment_kind(request.form.get('tx_kind', 'buy'))
        quantity = parse_decimal_or_none(request.form.get('quantity', '0')) or Decimal('0')
        unit_price = parse_decimal_or_none(request.form.get('unit_price', '0')) or Decimal('0')
        fees = parse_decimal_or_none(request.form.get('fees', '0')) or Decimal('0')
        notes = request.form.get('notes', '').strip() or None

        asset = InvestmentAsset.query.get(asset_id or 0)
        if not asset:
            flash('Please choose a valid asset.', 'warning')
            return redirect(url_for('investments'))

        if tx_kind in {'buy', 'sell', 'sip'} and quantity <= 0:
            flash('Quantity must be greater than zero.', 'warning')
            return redirect(url_for('investments'))
        if tx_kind == 'dividend' and quantity <= 0 and unit_price <= 0:
            flash('Provide dividend amount in price or quantity/price.', 'warning')
            return redirect(url_for('investments'))

        tx_date_val = pd.to_datetime(tx_date_raw, errors='coerce')
        if pd.isna(tx_date_val):
            tx_date_val = datetime.utcnow()

        tx = InvestmentTransaction(
            asset_id=asset.id,
            tx_date=tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val,
            tx_kind=tx_kind,
            quantity=quantity,
            unit_price=unit_price,
            fees=fees,
            notes=notes,
            source='manual',
        )
        db.session.add(tx)
        db.session.commit()
        log_audit('investments.tx_added', f'asset={asset.symbol},kind={tx_kind}')
        flash('Investment transaction added.', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not add investment transaction: {exc}', 'danger')
    return redirect(url_for('investments'))


@app.route('/investments/sync/transactions', methods=['POST'])
@login_required
def sync_investment_transactions():
    upload = request.files.get('transaction_file')
    if not upload or not upload.filename:
        flash('Please choose a CSV/Excel transaction file.', 'warning')
        return redirect(url_for('investments'))

    try:
        if upload.filename.lower().endswith('.csv'):
            df = pd.read_csv(upload)
        else:
            df = pd.read_excel(upload)
        df = prepare_import_dataframe(df)

        symbol_col = resolve_investment_column(df, request.form.get('symbol_col', 'Symbol'), ['Symbol', 'Ticker', 'ISIN', 'Fund'])
        date_col = resolve_investment_column(df, request.form.get('date_col', 'Date'), ['Date', 'Transaction Date', 'Tx Date'])
        type_col = resolve_investment_column(df, request.form.get('type_col', 'Type'), ['Type', 'Action', 'Transaction Type'])
        qty_col = resolve_investment_column(df, request.form.get('qty_col', 'Quantity'), ['Quantity', 'Units', 'Qty'])
        price_col = resolve_investment_column(df, request.form.get('price_col', 'Price'), ['Price', 'Unit Price', 'NAV', 'Rate', 'Amount'])
        fees_col = resolve_investment_column(df, request.form.get('fees_col', 'Fees'), ['Fees', 'Charges', 'Brokerage'])
        name_col = resolve_investment_column(df, request.form.get('name_col', 'Name'), ['Name', 'Asset Name', 'Scheme'])
        class_col = resolve_investment_column(df, request.form.get('class_col', 'Class'), ['Class', 'Asset Class', 'Category'])
        currency_col = resolve_investment_column(df, request.form.get('currency_col', 'Currency'), ['Currency'])

        if not symbol_col:
            flash('Could not detect symbol column for sync.', 'warning')
            return redirect(url_for('investments'))

        imported = 0
        for _, row in df.iterrows():
            symbol = str(row.get(symbol_col, '')).strip().upper()
            if not symbol:
                continue

            asset = InvestmentAsset.query.filter(func.upper(InvestmentAsset.symbol) == symbol).first()
            if not asset:
                asset_name = str(row.get(name_col, '')).strip() if name_col else ''
                asset = InvestmentAsset(
                    symbol=symbol,
                    name=asset_name or symbol,
                    asset_class=normalize_asset_class(str(row.get(class_col, 'stock')) if class_col else 'stock'),
                    currency=(str(row.get(currency_col, 'INR')).strip().upper() if currency_col else 'INR') or 'INR',
                    last_price=0,
                    is_active=True,
                )
                db.session.add(asset)
                db.session.flush()

            tx_kind = normalize_investment_kind(str(row.get(type_col, 'buy')) if type_col else 'buy')
            quantity = parse_decimal_or_none(row.get(qty_col, 0) if qty_col else 0) or Decimal('0')
            unit_price = parse_decimal_or_none(row.get(price_col, 0) if price_col else 0) or Decimal('0')
            fees = parse_decimal_or_none(row.get(fees_col, 0) if fees_col else 0) or Decimal('0')
            tx_date_val = pd.to_datetime(row.get(date_col), errors='coerce') if date_col else datetime.utcnow()
            if pd.isna(tx_date_val):
                tx_date_val = datetime.utcnow()

            if tx_kind in {'buy', 'sell', 'sip'} and quantity <= 0:
                continue
            if tx_kind == 'dividend' and quantity <= 0 and unit_price <= 0:
                continue

            tx = InvestmentTransaction(
                asset_id=asset.id,
                tx_date=tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val,
                tx_kind=tx_kind,
                quantity=quantity,
                unit_price=unit_price,
                fees=fees,
                notes='Synced from file',
                source='sync',
            )
            db.session.add(tx)
            imported += 1

        db.session.commit()
        log_audit('investments.tx_sync', f'imported={imported}')
        flash(f'Synced {imported} investment transaction(s).', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Investment sync failed: {exc}', 'danger')
    return redirect(url_for('investments'))


@app.route('/investments/sync/prices', methods=['POST'])
@login_required
def sync_investment_prices():
    upload = request.files.get('price_file')
    if not upload or not upload.filename:
        flash('Please choose a CSV/Excel price file.', 'warning')
        return redirect(url_for('investments'))

    try:
        if upload.filename.lower().endswith('.csv'):
            df = pd.read_csv(upload)
        else:
            df = pd.read_excel(upload)
        df = prepare_import_dataframe(df)

        symbol_col = resolve_investment_column(df, request.form.get('symbol_col', 'Symbol'), ['Symbol', 'Ticker', 'ISIN', 'Fund'])
        price_col = resolve_investment_column(df, request.form.get('price_col', 'Last Price'), ['Last Price', 'Price', 'NAV', 'LTP', 'Close'])
        date_col = resolve_investment_column(df, request.form.get('date_col', 'Date'), ['Date', 'As Of Date'])
        if not symbol_col or not price_col:
            flash('Could not detect symbol/price columns in price sync file.', 'warning')
            return redirect(url_for('investments'))

        updated = 0
        for _, row in df.iterrows():
            symbol = str(row.get(symbol_col, '')).strip().upper()
            if not symbol:
                continue
            price = parse_decimal_or_none(row.get(price_col, 0))
            if price is None or price <= 0:
                continue

            asset = InvestmentAsset.query.filter(func.upper(InvestmentAsset.symbol) == symbol).first()
            if not asset:
                asset = InvestmentAsset(
                    symbol=symbol,
                    name=symbol,
                    asset_class='other',
                    currency='INR',
                    last_price=price,
                    last_price_at=datetime.utcnow().date(),
                    is_active=True,
                )
                db.session.add(asset)
            else:
                asset.last_price = price

            as_of = pd.to_datetime(row.get(date_col), errors='coerce') if date_col else datetime.utcnow()
            if pd.isna(as_of):
                asset.last_price_at = datetime.utcnow().date()
            else:
                asset.last_price_at = as_of.date() if hasattr(as_of, 'date') else datetime.utcnow().date()
            updated += 1

        db.session.commit()
        log_audit('investments.price_sync', f'updated={updated}')
        flash(f'Updated prices for {updated} asset(s).', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Price sync failed: {exc}', 'danger')
    return redirect(url_for('investments'))


def first_day_of_month(any_date: date) -> date:
    return any_date.replace(day=1)


def first_day_previous_month(any_date: date) -> date:
    return add_months(any_date.replace(day=1), -1)


@app.route('/analytics')
@login_required
def analytics():
    user_id = current_user_id()
    today = datetime.utcnow().date()
    current_month_start = first_day_of_month(today)
    previous_month_start = first_day_previous_month(today)
    next_month_start = add_months(current_month_start, 1)

    monthly_transactions = transactions_for_user_query(user_id).order_by(Transaction.tx_date.asc()).all()
    month_map: dict[str, dict[str, Decimal]] = {}
    for tx in monthly_transactions:
        month_key = tx.tx_date.strftime('%Y-%m')
        if month_key not in month_map:
            month_map[month_key] = {'income': Decimal('0'), 'expense': Decimal('0')}
        if tx.tx_type == 'income':
            month_map[month_key]['income'] += Decimal(tx.amount or 0)
        elif tx.tx_type == 'expense':
            month_map[month_key]['expense'] += Decimal(tx.amount or 0)

    ordered_month_keys = sorted(month_map.keys())
    trend_labels = ordered_month_keys
    income_series = [float(month_map[key]['income']) for key in ordered_month_keys]
    expense_series = [float(month_map[key]['expense']) for key in ordered_month_keys]
    net_series = [round(i - e, 2) for i, e in zip(income_series, expense_series)]
    savings_rate_series = []
    for idx in range(len(trend_labels)):
        income_val = Decimal(income_series[idx] if idx < len(income_series) else 0)
        expense_val = Decimal(expense_series[idx] if idx < len(expense_series) else 0)
        if income_val > 0:
            savings_rate_series.append(float(((income_val - expense_val) / income_val) * Decimal('100')))
        else:
            savings_rate_series.append(0.0)

    recent_net = net_series[-3:] if len(net_series) >= 3 else net_series
    avg_monthly_net = (sum(recent_net) / len(recent_net)) if recent_net else 0.0
    if avg_monthly_net < 0 and len(expense_series) >= 1:
        recent_expense = expense_series[-3:] if len(expense_series) >= 3 else expense_series
        expense_anchor = (sum(recent_expense) / len(recent_expense)) if recent_expense else 0.0
        floor_limit = -(expense_anchor * 0.35)
        avg_monthly_net = max(avg_monthly_net, floor_limit)
    net_volatility = statistics.pstdev(recent_net) if len(recent_net) >= 2 else abs(avg_monthly_net) * 0.18
    starting_balance = float(
        accounts_for_user_query(user_id).with_entities(func.coalesce(func.sum(Account.opening_balance), 0)).scalar() or 0
    ) + sum(net_series)
    starting_balance = max(0.0, starting_balance)
    forecast_labels = []
    forecast_balance_series = []
    forecast_upper_series = []
    forecast_lower_series = []
    running_balance = starting_balance
    for idx in range(1, 7):
        month_date = add_months(current_month_start, idx)
        forecast_labels.append(month_key_for(month_date))
        running_balance = max(0.0, running_balance + avg_monthly_net)
        forecast_balance_series.append(round(running_balance, 2))
        band = float(net_volatility) * (idx ** 0.5)
        forecast_upper_series.append(round(max(0.0, running_balance + band), 2))
        forecast_lower_series.append(round(max(0.0, running_balance - band), 2))

    current_month_by_category = (
        db.session.query(
            Category.id.label('category_id'),
            Category.name.label('name'),
            Category.color.label('color'),
            func.sum(Transaction.amount).label('spent'),
        )
        .join(Transaction, Transaction.category_id == Category.id)
        .join(Account, Transaction.account_id == Account.id)
        .filter(
            Account.user_id == user_id,
            Transaction.tx_type == 'expense',
            Transaction.tx_date >= current_month_start,
            Transaction.tx_date < next_month_start,
        )
        .group_by(Category.id, Category.name, Category.color)
        .all()
    )
    previous_month_by_category = (
        db.session.query(
            Category.id.label('category_id'),
            func.sum(Transaction.amount).label('spent'),
        )
        .join(Transaction, Transaction.category_id == Category.id)
        .join(Account, Transaction.account_id == Account.id)
        .filter(
            Account.user_id == user_id,
            Transaction.tx_type == 'expense',
            Transaction.tx_date >= previous_month_start,
            Transaction.tx_date < current_month_start,
        )
        .group_by(Category.id)
        .all()
    )
    prev_map = {row.category_id: Decimal(row.spent or 0) for row in previous_month_by_category}
    leak_insights = []
    for row in current_month_by_category:
        current_spent = Decimal(row.spent or 0)
        previous_spent = prev_map.get(row.category_id, Decimal('0'))
        delta = current_spent - previous_spent
        if delta > 0:
            leak_insights.append(
                {
                    'category': row.name,
                    'current': current_spent,
                    'previous': previous_spent,
                    'delta': delta,
                }
            )
    leak_insights.sort(key=lambda item: item['delta'], reverse=True)
    leak_insights = leak_insights[:6]

    top_category_rows = sorted(
        current_month_by_category,
        key=lambda row: Decimal(row.spent or 0),
        reverse=True,
    )[:8]
    category_mix_labels = [row.name for row in top_category_rows]
    category_mix_values = [float(row.spent or 0) for row in top_category_rows]
    category_mix_colors = []
    category_mix_palette = ['#06b6d4', '#22c55e', '#f97316', '#a855f7', '#ef4444', '#14b8a6', '#eab308', '#3b82f6']
    for idx, row in enumerate(top_category_rows):
        raw_color = str(row.color or '').strip().lower()
        if raw_color and raw_color not in {'#6c757d', '#808080', '#7a7a7a', 'gray', 'grey'}:
            category_mix_colors.append(row.color)
        else:
            category_mix_colors.append(category_mix_palette[idx % len(category_mix_palette)])

    weekday_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    weekday_spend_values = [0.0 for _ in weekday_labels]
    recent_expense_transactions = (
        transactions_for_user_query(user_id)
        .filter(
            Transaction.tx_type == 'expense',
            Transaction.tx_date >= today - timedelta(days=180),
        )
        .all()
    )
    for tx in recent_expense_transactions:
        weekday_index = tx.tx_date.weekday()
        if 0 <= weekday_index <= 6:
            weekday_spend_values[weekday_index] += float(tx.amount or 0)

    daily_expenses = (
        db.session.query(
            func.date(Transaction.tx_date).label('tx_day'),
            func.sum(Transaction.amount).label('total'),
        )
        .join(Account, Transaction.account_id == Account.id)
        .filter(
            Account.user_id == user_id,
            Transaction.tx_type == 'expense',
            Transaction.tx_date >= today - timedelta(days=90),
        )
        .group_by('tx_day')
        .order_by('tx_day')
        .all()
    )
    anomaly_items = []
    daily_totals = [float(row.total or 0) for row in daily_expenses]
    if len(daily_totals) >= 5:
        mean_val = statistics.mean(daily_totals)
        std_val = statistics.pstdev(daily_totals)
        if std_val > 0:
            threshold = mean_val + (2 * std_val)
            for row in daily_expenses:
                total = float(row.total or 0)
                if total >= threshold:
                    anomaly_items.append(
                        {
                            'day': row.tx_day,
                            'total': total,
                            'threshold': threshold,
                        }
                    )
    anomaly_items = anomaly_items[-10:]

    month_key = month_key_for(current_month_start)
    budgets = (
        MonthlyBudget.query
        .join(Category, MonthlyBudget.category_id == Category.id)
        .filter(MonthlyBudget.month_key == month_key, MonthlyBudget.user_id == user_id)
        .order_by(Category.name.asc())
        .all()
    )
    spent_map = {row.category_id: Decimal(row.spent or 0) for row in current_month_by_category}
    budget_progress = []
    overspend_alerts = []
    for budget in budgets:
        spent = spent_map.get(budget.category_id, Decimal('0'))
        limit = Decimal(budget.amount_limit or 0)
        percent = float((spent / limit) * Decimal('100')) if limit > 0 else 0.0
        progress_item = {
            'budget': budget,
            'spent': spent,
            'remaining': max(Decimal('0'), limit - spent),
            'percent': percent,
            'overspend': spent > limit if limit > 0 else False,
        }
        budget_progress.append(progress_item)
        if progress_item['overspend']:
            overspend_alerts.append(progress_item)

    savings_goals = SavingsGoal.query.filter_by(user_id=user_id).order_by(SavingsGoal.created_at.desc()).all()
    goal_cards = []
    for goal in savings_goals:
        target = Decimal(goal.target_amount or 0)
        saved = Decimal(goal.current_saved or 0)
        pct = float((saved / target) * Decimal('100')) if target > 0 else 0.0
        goal_cards.append({'goal': goal, 'percent': min(100.0, pct)})

    return render_template(
        'analytics.html',
        trend_labels=trend_labels,
        income_series=income_series,
        expense_series=expense_series,
        net_series=net_series,
        savings_rate_series=savings_rate_series,
        forecast_labels=forecast_labels,
        forecast_balance_series=forecast_balance_series,
        forecast_upper_series=forecast_upper_series,
        forecast_lower_series=forecast_lower_series,
        avg_monthly_net=avg_monthly_net,
        category_mix_labels=category_mix_labels,
        category_mix_values=category_mix_values,
        category_mix_colors=category_mix_colors,
        weekday_labels=weekday_labels,
        weekday_spend_values=weekday_spend_values,
        leak_insights=leak_insights,
        anomaly_items=anomaly_items,
        budget_progress=budget_progress,
        overspend_alerts=overspend_alerts,
        goals=goal_cards,
        month_key=month_key,
        categories=Category.query.filter_by(is_active=True).order_by(Category.name.asc()).all(),
    )


@app.route('/analytics/budgets', methods=['POST'])
@login_required
def create_budget():
    user_id = current_user_id()
    try:
        month_key = request.form.get('month_key', '').strip()
        category_id = request.form.get('category_id', type=int)
        amount_limit = parse_decimal_or_none(request.form.get('amount_limit', ''))
        if not month_key or not category_id or amount_limit is None or amount_limit <= 0:
            flash('Enter valid budget details.', 'warning')
            return redirect(url_for('analytics'))

        budget = MonthlyBudget.query.filter_by(month_key=month_key, category_id=category_id, user_id=user_id).first()
        if budget:
            budget.amount_limit = amount_limit
        else:
            budget = MonthlyBudget(month_key=month_key, category_id=category_id, amount_limit=amount_limit, user_id=user_id)
            db.session.add(budget)
        db.session.commit()
        flash('Category budget saved.', 'success')
        log_audit('analytics.budget_saved', f'month={month_key},category={category_id}')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not save budget: {exc}', 'danger')
    return redirect(url_for('analytics'))


@app.route('/analytics/budgets/<int:budget_id>/delete', methods=['POST'])
@login_required
def delete_budget(budget_id: int):
    user_id = current_user_id()
    budget = MonthlyBudget.query.filter_by(id=budget_id, user_id=user_id).first_or_404()
    db.session.delete(budget)
    db.session.commit()
    flash('Budget deleted.', 'success')
    log_audit('analytics.budget_deleted', f'budget_id={budget_id}')
    return redirect(url_for('analytics'))


@app.route('/analytics/goals', methods=['POST'])
@login_required
def create_goal():
    user_id = current_user_id()
    try:
        name = request.form.get('name', '').strip()
        target_amount = parse_decimal_or_none(request.form.get('target_amount', ''))
        initial_saved = parse_decimal_or_none(request.form.get('current_saved', '0')) or Decimal('0')
        target_date_val = request.form.get('target_date', '').strip()
        target_date = datetime.strptime(target_date_val, '%Y-%m-%d').date() if target_date_val else None
        if not name or target_amount is None or target_amount <= 0:
            flash('Enter valid goal details.', 'warning')
            return redirect(url_for('analytics'))

        goal = SavingsGoal(
            name=name,
            target_amount=target_amount,
            current_saved=max(Decimal('0'), initial_saved),
            target_date=target_date,
            is_completed=initial_saved >= target_amount,
            user_id=user_id,
        )
        db.session.add(goal)
        db.session.commit()
        flash('Savings goal created.', 'success')
        log_audit('analytics.goal_created', f'name={name}')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not create goal: {exc}', 'danger')
    return redirect(url_for('analytics'))


@app.route('/analytics/goals/<int:goal_id>/contribute', methods=['POST'])
@login_required
def contribute_goal(goal_id: int):
    user_id = current_user_id()
    goal = SavingsGoal.query.filter_by(id=goal_id, user_id=user_id).first_or_404()
    amount = parse_decimal_or_none(request.form.get('amount', ''))
    if amount is None or amount <= 0:
        flash('Enter a valid contribution amount.', 'warning')
        return redirect(url_for('analytics'))
    goal.current_saved = Decimal(goal.current_saved or 0) + amount
    goal.is_completed = Decimal(goal.current_saved) >= Decimal(goal.target_amount)
    db.session.commit()
    flash('Contribution added to goal.', 'success')
    log_audit('analytics.goal_contribution', f'goal_id={goal.id},amount={amount}')
    return redirect(url_for('analytics'))


@app.route('/analytics/goals/<int:goal_id>/toggle', methods=['POST'])
@login_required
def toggle_goal(goal_id: int):
    user_id = current_user_id()
    goal = SavingsGoal.query.filter_by(id=goal_id, user_id=user_id).first_or_404()
    goal.is_completed = not goal.is_completed
    db.session.commit()
    flash('Goal status updated.', 'success')
    log_audit('analytics.goal_toggle', f'goal_id={goal.id},state={goal.is_completed}')
    return redirect(url_for('analytics'))


def get_records_filters_from_request() -> dict[str, object]:
    transfers = request.args.get('transfers', 'include').strip().lower()
    if transfers not in {'include', 'exclude', 'only'}:
        transfers = 'include'

    return {
        'account_id': request.args.get('account_id', type=int),
        'category_id': request.args.get('category_id', type=int),
        'tx_type': request.args.get('tx_type', '').strip(),
        'currency': request.args.get('currency', '').strip().upper(),
        'label': request.args.get('label', '').strip(),
        'record_state': request.args.get('record_state', '').strip(),
        'transfers': transfers,
        'min_amount': request.args.get('min_amount', '').strip(),
        'max_amount': request.args.get('max_amount', '').strip(),
        'search': request.args.get('search', '').strip(),
        'sort': request.args.get('sort', 'date_desc').strip(),
    }


def apply_records_filters(query, filters: dict[str, object]):
    account_id = filters.get('account_id')
    category_id = filters.get('category_id')
    tx_type = filters.get('tx_type', '')
    currency = filters.get('currency', '')
    label = filters.get('label', '')
    record_state = filters.get('record_state', '')
    transfers = filters.get('transfers', 'include')
    min_amount = parse_decimal_or_none(filters.get('min_amount', ''))
    max_amount = parse_decimal_or_none(filters.get('max_amount', ''))
    search = filters.get('search', '')

    if account_id:
        query = query.filter(Transaction.account_id == account_id)
    if category_id:
        query = query.filter(Transaction.category_id == category_id)
    if currency:
        query = query.filter(func.upper(Account.currency) == str(currency).upper())
    if tx_type:
        query = query.filter(Transaction.tx_type == tx_type)
    if transfers == 'exclude':
        query = query.filter(Transaction.tx_type != 'transfer')
    elif transfers == 'only':
        query = query.filter(Transaction.tx_type == 'transfer')

    if min_amount is not None and max_amount is not None and min_amount > max_amount:
        min_amount, max_amount = max_amount, min_amount
    if min_amount is not None:
        query = query.filter(Transaction.amount >= min_amount)
    if max_amount is not None:
        query = query.filter(Transaction.amount <= max_amount)

    if label == 'upi':
        query = query.filter(Transaction.description.ilike('%UPI-%'))
    elif label == 'with_reference':
        query = query.filter(
            Transaction.reference_no.isnot(None),
            func.trim(Transaction.reference_no) != '',
        )
    elif label == 'with_notes':
        query = query.filter(
            Transaction.notes.isnot(None),
            func.trim(Transaction.notes) != '',
        )
    elif label == 'without_notes':
        query = query.filter(
            db.or_(Transaction.notes.is_(None), func.trim(Transaction.notes) == ''),
        )

    if record_state in {'manual', 'import'}:
        query = query.filter(Transaction.source == record_state)

    if search:
        like = f'%{search}%'
        query = query.filter(
            db.or_(
                Transaction.description.ilike(like),
                Transaction.notes.ilike(like),
                Transaction.payee.ilike(like),
                Transaction.reference_no.ilike(like),
            )
        )
    return query


def parse_selected_transaction_ids_from_request() -> list[int]:
    ids: list[int] = []
    for raw_value in request.form.getlist('selected_ids'):
        text = str(raw_value or '').strip()
        if not text:
            continue
        if text.isdigit():
            ids.append(int(text))

    csv_ids = str(request.form.get('selected_ids_csv', '') or '').strip()
    if csv_ids:
        for part in csv_ids.split(','):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))

    unique_ids = sorted(set(ids))
    return unique_ids


def build_records_export_rows(transactions: list[Transaction]) -> list[dict]:
    tx_ids = [tx.id for tx in transactions]
    meta_map: dict[int, TransactionMeta] = {}
    if tx_ids:
        meta_rows = TransactionMeta.query.filter(TransactionMeta.transaction_id.in_(tx_ids)).all()
        meta_map = {row.transaction_id: row for row in meta_rows}

    rows = []
    for tx in transactions:
        signed_amount = Decimal(tx.amount) if tx.tx_type == 'income' else -Decimal(tx.amount)
        meta = meta_map.get(tx.id)
        rows.append(
            {
                'Date': tx.tx_date.strftime('%Y-%m-%d %H:%M:%S'),
                'Description': normalize_import_description(tx.description),
                'Account': tx.account.name,
                'Category': tx.category.name,
                'Type': tx.tx_type.title(),
                'Amount': float(signed_amount),
                'Notes': tx.notes or '',
                'Payee': tx.payee or '',
                'Label': (meta.label if meta and meta.label else ''),
                'Payment Type': (meta.payment_type if meta and meta.payment_type else ''),
                'Reference No': tx.reference_no or '',
                'Source': tx.source,
            }
        )
    return rows


def parse_decimal_or_none(value: object) -> Optional[Decimal]:
    text = str(value or '').replace(',', '').strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:
        return None


def resolve_records_sort(sort: str):
    sort_map = {
        'date_desc': Transaction.tx_date.desc(),
        'date_asc': Transaction.tx_date.asc(),
        'desc_asc': func.lower(Transaction.description).asc(),
        'desc_desc': func.lower(Transaction.description).desc(),
        'amount_desc': Transaction.amount.desc(),
        'amount_asc': Transaction.amount.asc(),
    }
    if sort not in sort_map:
        return 'date_desc', sort_map['date_desc']
    return sort, sort_map[sort]


@app.route('/transactions/add', methods=['GET', 'POST'])
@login_required
def add_transaction():
    user_id = current_user_id()
    accounts = accounts_for_user_query(user_id).order_by(Account.name).all()
    categories = Category.query.filter_by(is_active=True).order_by(Category.kind, Category.name).all()
    default_tx_date = request.form.get('tx_date', datetime.utcnow().strftime('%Y-%m-%dT%H:%M'))

    if request.method == 'POST':
        try:
            tx_date = datetime.strptime(request.form['tx_date'], '%Y-%m-%dT%H:%M')
            account = accounts_for_user_query(user_id).filter(Account.id == int(request.form['account_id'])).first()
            if not account:
                flash('Please choose a valid account for your profile.', 'warning')
                return redirect(url_for('add_transaction'))
            transaction = Transaction(
                tx_date=tx_date,
                description=request.form['description'].strip(),
                amount=Decimal(request.form['amount']),
                tx_type=request.form['tx_type'],
                notes=request.form.get('notes', '').strip() or None,
                payee=request.form.get('payee', '').strip() or None,
                reference_no=request.form.get('reference_no', '').strip() or None,
                account_id=account.id,
                category_id=int(request.form['category_id']),
                user_id=user_id,
                source='manual',
            )
            db.session.add(transaction)
            db.session.commit()
            flash('Transaction added successfully.', 'success')
            return redirect(url_for('records'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not save transaction: {exc}', 'danger')

    return render_template(
        'add_transaction.html',
        accounts=accounts,
        categories=categories,
        default_tx_date=default_tx_date,
    )


@app.route('/transactions/<int:transaction_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_transaction(transaction_id: int):
    user_id = current_user_id()
    transaction = transactions_for_user_query(user_id).filter(Transaction.id == transaction_id).first_or_404()
    accounts = accounts_for_user_query(user_id).order_by(Account.name).all()
    categories = Category.query.filter_by(is_active=True).order_by(Category.kind, Category.name).all()

    if request.method == 'POST':
        try:
            transaction.tx_date = datetime.strptime(request.form['tx_date'], '%Y-%m-%dT%H:%M')
            transaction.description = request.form['description'].strip()
            transaction.amount = Decimal(request.form['amount'])
            transaction.tx_type = request.form['tx_type']
            transaction.notes = request.form.get('notes', '').strip() or None
            transaction.payee = request.form.get('payee', '').strip() or None
            transaction.reference_no = request.form.get('reference_no', '').strip() or None
            account = accounts_for_user_query(user_id).filter(Account.id == int(request.form['account_id'])).first()
            if not account:
                flash('Please choose a valid account for your profile.', 'warning')
                return redirect(url_for('edit_transaction', transaction_id=transaction_id))
            transaction.account_id = account.id
            transaction.category_id = int(request.form['category_id'])

            db.session.commit()
            log_audit('records.transaction_edited', f'tx_id={transaction.id}')
            flash('Transaction updated successfully.', 'success')
            return redirect(url_for('records'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not update transaction: {exc}', 'danger')

    return render_template(
        'edit_transaction.html',
        transaction=transaction,
        accounts=accounts,
        categories=categories,
    )


@app.route('/transactions/<int:transaction_id>/delete', methods=['POST'])
@login_required
def delete_transaction(transaction_id: int):
    user_id = current_user_id()
    transaction = transactions_for_user_query(user_id).filter(Transaction.id == transaction_id).first_or_404()
    try:
        TransactionMeta.query.filter_by(transaction_id=transaction.id).delete(synchronize_session=False)
        db.session.delete(transaction)
        db.session.commit()
        log_audit('records.transaction_deleted', f'tx_id={transaction_id}')
        flash('Transaction deleted successfully.', 'success')
    except Exception as exc:
        db.session.rollback()
        flash(f'Could not delete transaction: {exc}', 'danger')
    return redirect(url_for('records'))


@app.route('/accounts/add', methods=['GET', 'POST'])
@login_required
def add_account():
    user_id = current_user_id()
    if request.method == 'POST':
        try:
            requested_name = request.form['name'].strip()
            unique_name = next_available_account_name(requested_name, user_id)
            if unique_name != requested_name:
                flash(f'Account name already existed. Saved as "{unique_name}".', 'info')
            account = Account(
                name=unique_name,
                account_type=request.form['account_type'],
                opening_balance=Decimal(request.form['opening_balance'] or '0'),
                currency=request.form['currency'].strip() or 'INR',
                user_id=user_id,
            )
            db.session.add(account)
            db.session.commit()
            flash('Account created successfully.', 'success')
            return redirect(url_for('dashboard'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not create account: {exc}', 'danger')
    return render_template('add_account.html')


@app.route('/categories')
@login_required
def categories():
    all_categories = Category.query.order_by(Category.kind, Category.name).all()
    return render_template('categories.html', categories=all_categories)


@app.route('/categories/add', methods=['GET', 'POST'])
@login_required
def add_category():
    if request.method == 'POST':
        try:
            name = request.form['name'].strip()
            kind = request.form['kind'].strip()
            icon = request.form['icon'].strip() or 'bi bi-tag'
            color = request.form['color'].strip() or '#6c757d'
            is_active = True if request.form.get('is_active') == 'on' else False

            exists = Category.query.filter(func.lower(Category.name) == name.lower()).first()
            if exists:
                flash('Category already exists.', 'warning')
                return redirect(url_for('add_category'))

            category = Category(
                name=name,
                kind=kind,
                icon=icon,
                color=color,
                is_active=is_active,
            )
            db.session.add(category)
            db.session.commit()
            flash('Category added successfully.', 'success')
            return redirect(url_for('categories'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Could not create category: {exc}', 'danger')

    return render_template('add_category.html', icon_options=ICON_OPTIONS)


@app.route('/categories/<int:category_id>/toggle', methods=['POST'])
@login_required
def toggle_category(category_id: int):
    category = Category.query.get_or_404(category_id)
    category.is_active = not category.is_active
    db.session.commit()
    flash('Category status updated.', 'success')
    return redirect(url_for('categories'))


@app.route('/import', methods=['GET', 'POST'])
@login_required
def import_statement():
    preview = []
    headers = []
    user_id = current_user_id()

    if request.method == 'POST':
        file = request.files.get('statement_file')
        account_id = request.form.get('account_id', type=int)
        date_col = request.form.get('date_col', 'Date')
        desc_col = request.form.get('desc_col', 'Description')
        amount_col = request.form.get('amount_col', 'Amount')
        type_col = request.form.get('type_col', 'Type')
        category_col = request.form.get('category_col', '')

        account = accounts_for_user_query(user_id).filter(Account.id == account_id).first() if account_id else None
        if account is None:
            flash('Please choose one of your accounts before importing.', 'warning')
            return redirect(url_for('import_statement'))

        if not file or not file.filename:
            flash('Please choose a CSV or Excel file.', 'warning')
            return redirect(url_for('import_statement'))

        try:
            if file.filename.lower().endswith('.csv'):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            df = prepare_import_dataframe(df)
            headers = list(df.columns)
            preview = df.head(10).to_dict(orient='records')

            resolved_date_col = resolve_import_column(df, date_col, DATE_COLUMN_ALIASES)
            resolved_desc_col = resolve_import_column(df, desc_col, DESC_COLUMN_ALIASES)
            resolved_amount_col = resolve_import_column(df, amount_col, AMOUNT_COLUMN_ALIASES)
            resolved_type_col = resolve_import_column(df, type_col, TYPE_COLUMN_ALIASES)
            resolved_category_col = resolve_import_column(df, category_col, CATEGORY_COLUMN_ALIASES)
            resolved_reference_col = resolve_import_column(df, '', REFERENCE_COLUMN_ALIASES)

            if 'do_import' in request.form:
                missing_required = []
                if not resolved_date_col:
                    missing_required.append('Date')
                if not resolved_desc_col:
                    missing_required.append('Description')
                if not resolved_amount_col:
                    missing_required.append('Amount')

                if missing_required:
                    flash(
                        'Could not auto-detect required columns: '
                        + ', '.join(missing_required)
                        + '. Please update the field mapping and preview again.',
                        'warning',
                    )
                    return render_template(
                        'import.html',
                        accounts=accounts_for_user_query(user_id).order_by(Account.name).all(),
                        preview=preview,
                        headers=headers,
                    )

                imported = 0
                skipped = 0
                for _, row in df.iterrows():
                    raw_description = row.get(resolved_desc_col, '') if resolved_desc_col else ''
                    amount, inferred_type = parse_amount_and_type(
                        row.get(resolved_amount_col, 0),
                        row.get(resolved_type_col, '') if resolved_type_col else '',
                        raw_description,
                    )
                    if amount is None:
                        skipped += 1
                        continue

                    tx_type = inferred_type or 'expense'

                    category_name = str(row.get(resolved_category_col, '')).strip() if resolved_category_col else ''
                    category = infer_category(category_name, tx_type)

                    tx_date_val = pd.to_datetime(row.get(resolved_date_col), errors='coerce', dayfirst=True)
                    if pd.isna(tx_date_val):
                        skipped += 1
                        continue

                    description = normalize_import_description(raw_description)

                    reference_no = (
                        str(row.get(resolved_reference_col, '')).strip()
                        if resolved_reference_col else ''
                    )
                    if not reference_no or reference_no.lower() == 'nan':
                        reference_no = None

                    transaction = Transaction(
                        tx_date=tx_date_val.to_pydatetime() if hasattr(tx_date_val, 'to_pydatetime') else tx_date_val,
                        description=description,
                        amount=amount,
                        tx_type=tx_type,
                        notes='Imported from file',
                        reference_no=reference_no,
                        account_id=account.id,
                        category_id=category.id,
                        user_id=user_id,
                        source='import',
                    )
                    db.session.add(transaction)
                    imported += 1

                if imported == 0:
                    db.session.rollback()
                    flash('No valid transaction rows found to import. Please check mapping/statement format.', 'warning')
                else:
                    db.session.commit()
                    flash(f'Imported {imported} transactions successfully. Skipped {skipped} rows.', 'success')
                    return redirect(url_for('records'))
        except Exception as exc:
            db.session.rollback()
            flash(f'Import failed: {exc}', 'danger')

    return render_template(
        'import.html',
        accounts=accounts_for_user_query(user_id).order_by(Account.name).all(),
        preview=preview,
        headers=headers,
    )


def normalize_import_key(value: object) -> str:
    return re.sub(r'[^a-z0-9]+', '', str(value or '').strip().lower())


def is_blank_cell(value: object) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    text = str(value).strip()
    return text == '' or text.lower() == 'nan'


def promote_header_row_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    columns = [str(col).strip() for col in df.columns]
    unnamed_columns = [
        col for col in columns
        if not col or col.lower().startswith('unnamed')
    ]
    if len(unnamed_columns) < max(1, len(columns) // 2):
        return df

    search_limit = min(len(df), 40)
    for idx in range(search_limit):
        row_values = [cell for cell in df.iloc[idx].tolist() if not is_blank_cell(cell)]
        if not row_values:
            continue

        keys = {normalize_import_key(cell) for cell in row_values}
        has_date = any(key in keys for key in ('date', 'transactiondate', 'valuedate', 'txndate'))
        has_desc = any(
            key in keys
            for key in (
                'description',
                'details',
                'narration',
                'merchant',
                'transactiondetails',
                'transactiondetail',
                'transactiondescription',
                'particulars',
            )
        )
        has_amount = any('amount' in key for key in keys)
        if not (has_date and has_desc and has_amount):
            continue

        header_values = df.iloc[idx].tolist()
        new_columns = []
        for col_idx, cell in enumerate(header_values):
            if is_blank_cell(cell):
                new_columns.append(f'col_{col_idx}')
            else:
                new_columns.append(str(cell).strip())

        promoted_df = df.iloc[idx + 1:].copy()
        promoted_df.columns = new_columns
        return promoted_df

    return df


def prepare_import_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = promote_header_row_if_needed(df)
    normalized_columns = []
    for idx, col in enumerate(df.columns):
        col_text = str(col).strip()
        normalized_columns.append(col_text if col_text else f'col_{idx}')
    df.columns = normalized_columns

    non_empty_columns = []
    for col in df.columns:
        if df[col].apply(lambda value: not is_blank_cell(value)).any():
            non_empty_columns.append(col)

    df = df[non_empty_columns].copy()
    return df.fillna('')


def resolve_import_column(df: pd.DataFrame, preferred: str, aliases: list[str]) -> Optional[str]:
    if preferred and preferred in df.columns:
        return preferred

    normalized_to_actual = {}
    for col in df.columns:
        normalized_to_actual[normalize_import_key(col)] = col

    if preferred:
        normalized_preferred = normalize_import_key(preferred)
        if normalized_preferred in normalized_to_actual:
            return normalized_to_actual[normalized_preferred]

    for alias in aliases:
        normalized_alias = normalize_import_key(alias)
        if normalized_alias in normalized_to_actual:
            return normalized_to_actual[normalized_alias]

    return None


def parse_amount_and_type(raw_amount: object, raw_type: object, raw_description: object = '') -> tuple[Optional[Decimal], str]:
    if is_blank_cell(raw_amount):
        return None, 'expense'

    amount_text = str(raw_amount).strip()
    marker_text = f'{amount_text} {raw_type or ""}'.lower()

    amount_match = re.search(r'-?\d[\d,]*(?:\.\d+)?', amount_text)
    numeric_text = amount_match.group(0) if amount_match else amount_text
    numeric_text = numeric_text.replace(',', '').strip()

    try:
        amount = Decimal(numeric_text)
    except Exception:
        return None, 'expense'

    if 'dr' in marker_text or 'debit' in marker_text:
        tx_type = 'expense'
    elif 'cr' in marker_text or 'credit' in marker_text:
        tx_type = 'income'
    else:
        requested_type = str(raw_type or '').strip().lower()
        if requested_type in {'income', 'expense', 'transfer'}:
            tx_type = requested_type
        elif amount < 0:
            tx_type = 'income'
        else:
            tx_type = 'expense'

    description_text = str(raw_description or '').strip().lower()
    if tx_type == 'expense':
        if any(marker in description_text for marker in ('payment received', 'bbps payment', 'card payment')):
            tx_type = 'transfer'
        elif any(marker in description_text for marker in ('refund', 'reversal', 'cashback', 'chargeback')):
            tx_type = 'income'

    return abs(amount), tx_type


def normalize_import_description(raw_description: object) -> str:
    description = str(raw_description or '').strip()
    if not description:
        return 'Imported transaction'

    compact = re.sub(r'\s+', ' ', description)
    if compact.upper().startswith('UPI-'):
        # Statement rows often look like:
        # UPI-..._<second-upi-segment>-MERCHANT NAME
        candidate = compact.split('_')[-1].strip()
        if '-' in candidate:
            merchant = candidate.rsplit('-', 1)[-1].strip(' -_')
            if merchant:
                return merchant

    return compact


def infer_category(category_name: str, tx_type: str) -> Category:
    if category_name:
        category = Category.query.filter(func.lower(Category.name) == category_name.lower()).first()
        if category:
            return category
        icon, color = CATEGORY_STYLE_MAP.get(category_name.strip().lower(), ('bi bi-tag', '#6c757d'))
        category = Category(
            name=category_name,
            kind=tx_type,
            icon=icon,
            color=color,
            is_active=True,
        )
        db.session.add(category)
        db.session.flush()
        return category

    default_map = {
        'income': 'Salary',
        'expense': 'Groceries',
        'transfer': 'Transfer',
    }
    fallback = default_map.get(tx_type, 'Groceries')
    return Category.query.filter_by(name=fallback).first()


@app.template_filter('currency')
def currency_filter(value: Optional[float | Decimal]) -> str:
    value = Decimal(value or 0)
    sign = '-' if value < 0 else ''
    value = abs(value)
    return f'{sign}₹{value:,.2f}'


@app.template_filter('clean_description')
def clean_description_filter(value: object) -> str:
    return normalize_import_description(value)


with app.app_context():
    db.create_all()
    run_ownership_schema_updates_safely()
    seed_defaults()


if __name__ == '__main__':
    app.run(
        host=os.getenv('APP_HOST', '0.0.0.0'),
        port=int(os.getenv('PORT', os.getenv('APP_PORT', '5000'))),
        debug=os.getenv('FLASK_DEBUG', '0') == '1',
    )
